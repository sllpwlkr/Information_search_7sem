#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <locale>
#include <codecvt>
#include <cwctype>
#include <cstdint>

namespace fs = std::filesystem;

static std::wstring utf8_to_wstring(const std::string& s) {
    static std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;
    return conv.from_bytes(s);
}

static std::string wstring_to_utf8(const std::wstring& ws) {
    static std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;
    return conv.to_bytes(ws);
}

static bool is_russian_letter(wchar_t c) {
    if (c == L'ё' || c == L'Ё') return true;
    return (c >= 0x0400 && c <= 0x04FF) || (c >= 0x0500 && c <= 0x052F);
}

static bool contains_cyrillic(const std::wstring& s) {
    for (wchar_t c : s) {
        if (is_russian_letter(c)) return true;
    }
    return false;
}

static wchar_t to_lower_ru(wchar_t c) {
    return std::towlower(c);
}

static std::wstring to_lower_ws(std::wstring s) {
    for (auto& ch : s) ch = to_lower_ru(ch);
    return s;
}

static bool is_all_digits_ws(const std::wstring& s) {
    if (s.empty()) return false;
    for (wchar_t c : s) {
        if (!(c >= L'0' && c <= L'9')) return false;
    }
    return true;
}

static bool is_vowel_ru(wchar_t c) {
    switch (c) {
        case L'а': case L'е': case L'и': case L'о': case L'у':
        case L'ы': case L'э': case L'ю': case L'я': case L'ё':
            return true;
        default:
            return false;
    }
}

static bool ends_with(const std::wstring& s, const std::wstring& suf) {
    if (s.size() < suf.size()) return false;
    return std::equal(suf.rbegin(), suf.rend(), s.rbegin());
}

static bool remove_suffix_if_ends(std::wstring& s, const std::wstring& suf) {
    if (!ends_with(s, suf)) return false;
    s.erase(s.size() - suf.size());
    return true;
}

static bool remove_any_suffix(std::wstring& s, const std::vector<std::wstring>& suffixes) {
    for (const auto& suf : suffixes) {
        if (remove_suffix_if_ends(s, suf)) return true;
    }
    return false;
}

static std::wstring stem_ru_porter(const std::wstring& token) {
    std::wstring w = to_lower_ws(token);

    for (auto& ch : w) if (ch == L'ё') ch = L'е';

    size_t rv = std::wstring::npos;
    for (size_t i = 0; i < w.size(); i++) {
        if (is_vowel_ru(w[i])) {
            rv = i + 1;
            break;
        }
    }
    if (rv == std::wstring::npos || rv >= w.size()) {
        return w;
    }

    std::wstring prefix = w.substr(0, rv);
    std::wstring r = w.substr(rv);

    const std::vector<std::wstring> perfective_1 = {L"ивши", L"ившись", L"ив", L"ившись", L"ивши"};
    const std::vector<std::wstring> perfective_2 = {L"вшись", L"вши", L"в"};

    const std::vector<std::wstring> reflexive = {L"ся", L"сь"};

    const std::vector<std::wstring> adjective = {
        L"ее", L"ие", L"ые", L"ое", L"ими", L"ыми", L"ей", L"ий", L"ый", L"ой",
        L"ем", L"им", L"ым", L"ом", L"его", L"ого", L"ему", L"ому",
        L"их", L"ых", L"ую", L"юю", L"ая", L"яя", L"ою", L"ею"
    };

    const std::vector<std::wstring> participle_1 = {L"ем", L"нн", L"вш", L"ющ", L"щ"};
    const std::vector<std::wstring> participle_2 = {L"ивш", L"ывш", L"ующ"};

    const std::vector<std::wstring> verb_1 = {
        L"ила", L"ыла", L"ена", L"ейте", L"уйте", L"ите", L"или", L"ыли",
        L"ей", L"уй", L"ил", L"ыл", L"им", L"ым", L"ен", L"ило", L"ыло",
        L"ено", L"ят", L"ует", L"уют", L"ит", L"ыт", L"ены", L"ить", L"ыть",
        L"ишь", L"ую", L"ю"
    };
    const std::vector<std::wstring> verb_2 = {
        L"ла", L"на", L"ете", L"йте", L"ли", L"й", L"л", L"ем", L"н",
        L"ло", L"но", L"ет", L"ют", L"ны", L"ть", L"ешь", L"нно"
    };

    const std::vector<std::wstring> noun = {
        L"а", L"ев", L"ов", L"ие", L"ье", L"е", L"иями", L"ями", L"ами",
        L"еи", L"ии", L"и", L"ией", L"ей", L"ой", L"ий", L"й",
        L"иям", L"ям", L"ием", L"ем", L"ам", L"ом", L"о",
        L"у", L"ах", L"иях", L"ях", L"ы", L"ь", L"ию", L"ью",
        L"ю", L"ия", L"ья", L"я"
    };

    const std::vector<std::wstring> derivational = {L"ост", L"ость"};

    const std::vector<std::wstring> superlative = {L"ейш", L"ейше"};

    bool removed = false;

    if (remove_any_suffix(r, perfective_1)) {
        removed = true;
    } else if (remove_any_suffix(r, perfective_2)) {
        removed = true;
    }

    if (!removed) {
        remove_any_suffix(r, reflexive);

        if (remove_any_suffix(r, adjective)) {
            if (!remove_any_suffix(r, participle_2)) {
                remove_any_suffix(r, participle_1);
            }
        } else {
            if (!remove_any_suffix(r, verb_1)) {
                if (!remove_any_suffix(r, verb_2)) {
                    remove_any_suffix(r, noun);
                }
            }
        }
    }

    remove_suffix_if_ends(r, L"и");

    if (ends_with(r, L"ость")) {
        bool has_vowel = false;
        for (size_t i = 0; i + 4 < r.size(); i++) {
            if (is_vowel_ru(r[i])) { has_vowel = true; break; }
        }
        if (has_vowel) remove_suffix_if_ends(r, L"ость");
    } else if (ends_with(r, L"ост")) {
        bool has_vowel = false;
        for (size_t i = 0; i + 3 < r.size(); i++) {
            if (is_vowel_ru(r[i])) { has_vowel = true; break; }
        }
        if (has_vowel) remove_suffix_if_ends(r, L"ост");
    }

    if (!remove_suffix_if_ends(r, L"ь")) {
        remove_any_suffix(r, superlative);
        if (ends_with(r, L"нн")) {
            r.erase(r.size() - 1);
        }
    }

    return prefix + r;
}

static bool parse_tokens_tsv_line(const std::string& line, std::string& doc_id, std::string& pos, std::string& token) {
    size_t p1 = line.find('\t');
    if (p1 == std::string::npos) return false;
    size_t p2 = line.find('\t', p1 + 1);
    if (p2 == std::string::npos) return false;

    doc_id = line.substr(0, p1);
    pos = line.substr(p1 + 1, p2 - (p1 + 1));
    token = line.substr(p2 + 1);
    if (!token.empty() && token.back() == '\r') token.pop_back();
    return !(doc_id.empty() || pos.empty() || token.empty());
}

int main(int argc, char** argv) {
    std::setlocale(LC_ALL, "C.UTF-8");

    fs::path in_path = "data/tokens/tokens.tsv";
    fs::path out_path = "data/tokens/tokens_stem.tsv";

    if (argc >= 2) in_path = argv[1];
    if (argc >= 3) out_path = argv[2];

    std::ifstream in(in_path, std::ios::binary);
    if (!in) {
        std::cerr << "ОШИБКА: не удалось открыть входной файл: " << in_path << "\n";
        return 1;
    }

    fs::create_directories(out_path.parent_path());

    std::ofstream out(out_path, std::ios::binary);
    if (!out) {
        std::cerr << "ОШИБКА: не удалось открыть выходной файл: " << out_path << "\n";
        return 1;
    }

    uint64_t total_read = 0;
    uint64_t total_written = 0;
    uint64_t changed = 0;
    uint64_t dropped_numeric = 0;

    std::string line;
    std::string doc_id, pos, token;

    while (std::getline(in, line)) {
        if (line.empty()) continue;
        if (!parse_tokens_tsv_line(line, doc_id, pos, token)) continue;

        total_read++;

        std::wstring wtok;
        try {
            wtok = utf8_to_wstring(token);
        } catch (...) {
            out << doc_id << "\t" << pos << "\t" << token << "\n";
            total_written++;
            continue;
        }

        if (is_all_digits_ws(wtok)) {
            dropped_numeric++;
            continue;
        }

        std::wstring stemmed = wtok;

        if (wtok.size() <= 3) {
            stemmed = to_lower_ws(wtok);
        }
        else if (contains_cyrillic(wtok)) {
            stemmed = stem_ru_porter(wtok);
        }
        else {
            stemmed = to_lower_ws(wtok);
        }

        std::string out_tok = wstring_to_utf8(stemmed);
        if (out_tok != token) changed++;

        out << doc_id << "\t" << pos << "\t" << out_tok << "\n";
        total_written++;
    }

    std::cout << "Вход: " << in_path << "\n";
    std::cout << "Выход: " << out_path << "\n";
    std::cout << "Прочитано токенов: " << total_read << "\n";
    std::cout << "Записано токенов: " << total_written << "\n";
    std::cout << "Удалено числовых токенов: " << dropped_numeric << "\n";
    std::cout << "Изменено токенов: " << changed << "\n";
    return 0;
}
