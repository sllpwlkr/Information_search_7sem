#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cctype>
#include <algorithm>
#include <chrono>
#include <json/json.h>
#include <set>

std::string to_lower(const std::string& s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

struct DirectIndex {
    std::string doc_id;
    std::string title;
    std::string url;
};

struct InvertedIndex {
    std::string term;
    std::vector<std::string> doc_ids;
};

void write_direct_index(const std::vector<DirectIndex>& direct_index, const std::string& filename) {
    std::ofstream out(filename, std::ios::binary);
    if (!out) {
        std::cerr << "Ошибка при открытии файла для записи прямого индекса!" << std::endl;
        return;
    }

    for (const auto& doc : direct_index) {
        uint64_t title_size = doc.title.size();
        out.write(reinterpret_cast<const char*>(&title_size), sizeof(title_size));
        out.write(doc.title.c_str(), title_size);
        uint64_t url_size = doc.url.size();
        out.write(reinterpret_cast<const char*>(&url_size), sizeof(url_size));
        out.write(doc.url.c_str(), url_size);
    }
    out.close();
}

void write_inverted_index(const std::vector<InvertedIndex>& inverted_index, const std::string& filename) {
    std::ofstream out(filename, std::ios::binary);
    if (!out) {
        std::cerr << "Ошибка при открытии файла для записи обратного индекса!" << std::endl;
        return;
    }

    for (const auto& term : inverted_index) {
        uint64_t term_size = term.term.size();
        out.write(reinterpret_cast<const char*>(&term_size), sizeof(term_size));
        out.write(term.term.c_str(), term_size);

        uint64_t doc_count = term.doc_ids.size();
        out.write(reinterpret_cast<const char*>(&doc_count), sizeof(doc_count));
        for (const auto& doc_id : term.doc_ids) {
            uint64_t doc_id_size = doc_id.size();
            out.write(reinterpret_cast<const char*>(&doc_id_size), sizeof(doc_id_size));
            out.write(doc_id.c_str(), doc_id_size);
        }
    }
    out.close();
}

void parse_tokens(const std::string& text, std::vector<std::string>& tokens) {
    std::string token;
    for (char ch : text) {
        if (std::isalnum(ch)) {
            token.push_back(ch);
        } else {
            if (!token.empty()) {
                tokens.push_back(to_lower(token));
                token.clear();
            }
        }
    }
    if (!token.empty()) {
        tokens.push_back(to_lower(token));
    }
}

void log_statistics(double total_time, uint64_t total_tokens, uint64_t total_docs, uint64_t total_terms, double avg_term_length) {
    std::ofstream log_file("/app/logs/indexing_log.txt", std::ios::app);
    if (!log_file.is_open()) {
        std::cerr << "Ошибка при открытии файла для логирования!" << std::endl;
        return;
    }

    log_file << "Статистика индексации:" << std::endl;
    log_file << "Общее время индексации: " << total_time << " секунд" << std::endl;
    log_file << "Количество документов: " << total_docs << std::endl;
    log_file << "Общее количество токенов: " << total_tokens << std::endl;
    log_file << "Количество термов (уникальных токенов): " << total_terms << std::endl;
    log_file << "Средняя длина терма: " << avg_term_length << std::endl;
    log_file << "Скорость индексации: " << total_tokens / total_time << " токенов в секунду" << std::endl;
    log_file << "Скорость индексации на один документ: " << total_tokens / total_docs << " токенов на документ" << std::endl;
    log_file << "Скорость индексации на килобайт текста: " << (total_tokens * 1.0) / (total_terms / 1024) << " токенов на килобайт текста" << std::endl;
    log_file << std::endl;
    log_file.close();

    std::cout << "Индексация завершена!" << std::endl;
    std::cout << "Общее время индексации: " << total_time << " секунд" << std::endl;
    std::cout << "Количество документов: " << total_docs << std::endl;
    std::cout << "Общее количество токенов: " << total_tokens << std::endl;
    std::cout << "Количество термов (уникальных токенов): " << total_terms << std::endl;
    std::cout << "Средняя длина терма: " << avg_term_length << std::endl;
}

int main() {
    std::cout << "Начинаем индексацию..." << std::endl;

    auto start_time = std::chrono::high_resolution_clock::now();

    std::ifstream corpus_file("data/corpus.jsonl");
    if (!corpus_file) {
        std::cerr << "Не удалось открыть файл corpus.jsonl!" << std::endl;
        return 1;
    }

    std::cout << "Файл с корпусом загружен." << std::endl;

    std::vector<DirectIndex> direct_index;
    std::vector<InvertedIndex> inverted_index;

    std::string line;
    uint64_t total_tokens = 0;
    uint64_t total_docs = 0;
    uint64_t total_terms = 0;

    std::set<std::string> doc_ids_set;

    while (std::getline(corpus_file, line)) {
        std::istringstream line_stream(line);
        Json::Reader reader;
        Json::Value doc_data;

        if (!reader.parse(line, doc_data)) {
            std::cerr << "Ошибка при парсинге строки JSON!" << std::endl;
            continue;
        }

        std::string doc_id = doc_data["doc_id"].asString();
        std::string title = doc_data["title"].asString();
        std::string url = doc_data["normalized_url"].asString();
        std::string clean_text = doc_data["clean_text"].asString();

        if (doc_ids_set.find(doc_id) != doc_ids_set.end()) {
            std::cerr << "Найден дубликат документа с ID: " << doc_id << std::endl;
        } else {
            doc_ids_set.insert(doc_id);
        }

        direct_index.push_back({doc_id, title, url});

        std::vector<std::string> tokens;
        parse_tokens(clean_text, tokens);
        total_tokens += tokens.size();

        for (const auto& token : tokens) {
            bool found = false;
            for (auto& inverted_entry : inverted_index) {
                if (inverted_entry.term == token) {
                    inverted_entry.doc_ids.push_back(doc_id);
                    found = true;
                    break;
                }
            }
            if (!found) {
                inverted_index.push_back({token, {doc_id}});
                total_terms++;
            }
        }

        total_docs++;
    }
    corpus_file.close();

    std::cout << "Индексация завершена. Запись в файлы..." << std::endl;

    write_direct_index(direct_index, "data/direct_index.bin");
    write_inverted_index(inverted_index, "data/inverted_index.bin");

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> total_duration = end_time - start_time;
    double total_time = total_duration.count();

    double avg_term_length = total_tokens / static_cast<double>(total_terms);

    log_statistics(total_time, total_tokens, total_docs, total_terms, avg_term_length);

    std::cout << "Булев индекс успешно создан!" << std::endl;
    return 0;
}
