#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <chrono>
#include <clocale>
#include <cwctype>
#include <locale>
#include <codecvt>

namespace fs = std::filesystem;

struct Stats {
    uint64_t total_tokens = 0;
    uint64_t total_token_chars = 0;
    uint64_t total_bytes_text = 0;
};

static bool is_cyrillic(wchar_t c) {
    return (c >= 0x0400 && c <= 0x04FF) || (c >= 0x0500 && c <= 0x052F) || (c == L'ё' || c == L'Ё');
}

static bool is_latin(wchar_t c) {
    return (c >= L'a' && c <= L'z') || (c >= L'A' && c <= L'Z');
}

static bool is_combining_mark(wchar_t c) {
    return (c >= 0x0300 && c <= 0x036F);
}

static bool is_digit(wchar_t c) {
    return (c >= L'0' && c <= L'9');
}

static bool is_alnum_ru(wchar_t c) {
    return is_digit(c) || is_latin(c) || is_cyrillic(c);
}

static wchar_t to_lower_ru(wchar_t c) {
    return std::towlower(c);
}

static bool extract_doc_id(const std::string& line, std::string& doc_id_out) {
    const std::string key = "\"doc_id\"";
    size_t p = line.find(key);
    if (p == std::string::npos) return false;
    p = line.find(':', p);
    if (p == std::string::npos) return false;
    p++;
    while (p < line.size() && line[p] == ' ') p++;

    if (p < line.size() && line[p] == '"') {
        p++;
        size_t e = line.find('"', p);
        if (e == std::string::npos) return false;
        doc_id_out = line.substr(p, e - p);
        return true;
    }

    size_t e = p;
    while (e < line.size() && (line[e] >= '0' && line[e] <= '9')) e++;
    if (e == p) return false;
    doc_id_out = line.substr(p, e - p);
    return true;
}

static bool extract_clean_text(const std::string& line, std::string& text_out) {
    const std::string key = "\"clean_text\"";
    size_t p = line.find(key);
    if (p == std::string::npos) return false;
    p = line.find(':', p);
    if (p == std::string::npos) return false;
    p++;
    while (p < line.size() && line[p] == ' ') p++;
    if (p >= line.size() || line[p] != '"') return false;
    p++;

    std::string result;
    result.reserve(4096);

    bool esc = false;
    for (size_t i = p; i < line.size(); i++) {
        char ch = line[i];
        if (esc) {
            switch (ch) {
                case 'n': result.push_back('\n'); break;
                case 't': result.push_back('\t'); break;
                case 'r': result.push_back('\r'); break;
                case '"': result.push_back('"'); break;
                case '\\': result.push_back('\\'); break;
                default: result.push_back(ch); break;
            }
            esc = false;
            continue;
        }
        if (ch == '\\') { esc = true; continue; }
        if (ch == '"') {
            text_out = std::move(result);
            return true;
        }
        result.push_back(ch);
    }
    return false;
}

static std::wstring utf8_to_wstring(const std::string& s) {
    static std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;
    return conv.from_bytes(s);
}

static std::string wstring_to_utf8(const std::wstring& ws) {
    static std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;
    return conv.to_bytes(ws);
}

static bool is_all_digits(const std::wstring& s) {
    if (s.empty()) return false;
    for (wchar_t c : s) {
        if (!(c >= L'0' && c <= L'9')) return false;
    }
    return true;
}

static void tokenize_text(
    const std::wstring& wtext,
    std::vector<std::wstring>& tokens,
    std::vector<uint32_t>& positions
) {
    tokens.clear();
    positions.clear();

    std::wstring cur;
    cur.reserve(32);

    uint32_t pos = 0;

    auto flush = [&]() {
        if (cur.empty()) return;

        const bool digits_only = is_all_digits(cur);
        if (digits_only || cur.size() >= 3) {
            tokens.push_back(cur);
            positions.push_back(pos++);
        }
        cur.clear();
    };

    const size_t n = wtext.size();
    for (size_t i = 0; i < n; i++) {
        wchar_t c = wtext[i];

        if (is_combining_mark(c)) {
            continue;
        }

        if (is_alnum_ru(c)) {
            cur.push_back(to_lower_ru(c));
            continue;
        }

        if (c == L'-') {
            bool left_ok = !cur.empty();
            bool right_ok = (i + 1 < n) && is_alnum_ru(wtext[i + 1]);
            if (left_ok && right_ok) {
                cur.push_back(L'-');
                continue;
            }
        }

        flush();
    }

    flush();
}

static void ensure_dir(const fs::path& p) {
    std::error_code ec;
    if (!fs::exists(p, ec)) {
        fs::create_directories(p, ec);
    }
}

int main(int argc, char** argv) {
    std::setlocale(LC_ALL, "C.UTF-8");

    if (argc < 3) {
        std::cerr << "Использование: tokenizer <input.jsonl> <output_dir>\n";
        std::cerr << "Пример: tokenizer data/corpus.jsonl data/tokens\n";
        return 1;
    }

    const fs::path input_path = argv[1];
    const fs::path out_dir = argv[2];

    ensure_dir(out_dir);

    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Не удалось открыть входной файл: " << input_path << "\n";
        return 1;
    }

    std::ofstream tokens_out(out_dir / "tokens.tsv", std::ios::binary);
    if (!tokens_out) {
        std::cerr << "Не удалось открыть tokens.tsv\n";
        return 1;
    }

    std::ofstream docs_idx(out_dir / "docs.idx", std::ios::binary);
    if (!docs_idx) {
        std::cerr << "Не удалось открыть docs.idx\n";
        return 1;
    }

    Stats stats;
    auto t0 = std::chrono::steady_clock::now();

    std::string line;
    uint64_t docs = 0;

    std::vector<std::wstring> tokens;
    std::vector<uint32_t> positions;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::string doc_id;
        std::string clean_text;

        if (!extract_doc_id(line, doc_id)) continue;
        if (!extract_clean_text(line, clean_text)) continue;

        stats.total_bytes_text += (uint64_t)clean_text.size();

        std::wstring wtext;
        try {
            wtext = utf8_to_wstring(clean_text);
        } catch (...) {
            continue;
        }

        tokenize_text(wtext, tokens, positions);

        std::streampos start_offset = tokens_out.tellp();
        uint64_t doc_token_count = 0;

        for (size_t i = 0; i < tokens.size(); i++) {
            const std::string tok_utf8 = wstring_to_utf8(tokens[i]);

            tokens_out << doc_id << "\t" << positions[i] << "\t" << tok_utf8 << "\n";

            stats.total_tokens++;
            stats.total_token_chars += (uint64_t)tokens[i].size();
            doc_token_count++;
        }

        docs_idx << doc_id << "\t" << (uint64_t)start_offset << "\t" << doc_token_count << "\n";

        docs++;
    }

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();

    double avg_len = stats.total_tokens ? (double)stats.total_token_chars / (double)stats.total_tokens : 0.0;
    double kb = (double)stats.total_bytes_text / 1024.0;
    double kb_per_sec = (sec > 0.0) ? (kb / sec) : 0.0;
    double us_per_kb = (kb > 0.0) ? (sec * 1e6 / kb) : 0.0;
    double tok_per_sec = (sec > 0.0) ? ((double)stats.total_tokens / sec) : 0.0;

    std::cout << "Обработано документов: " << docs << "\n";
    std::cout << "Общее количество токенов: " << stats.total_tokens << "\n";
    std::cout << "Средняя длина токена (символов): " << avg_len << "\n";
    std::cout << "Размер входных данных (clean_text): " << stats.total_bytes_text << " (" << kb << " KB)\n";
    std::cout << "Время: " << sec << " сек\n";
    std::cout << "Скорость: " << kb_per_sec << " KB/с\n";
    std::cout << "Скорость: " << us_per_kb << " мк/KB\n";
    std::cout << "Скорость: " << tok_per_sec << " токенов/с\n";
    std::cout << "Токены сохранены в: " << (out_dir / "tokens.tsv") << "\n";
    std::cout << "Индекс документов сохранен в: " << (out_dir / "docs.idx") << "\n";

    return 0;
}
