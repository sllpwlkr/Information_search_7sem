#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <set>
#include <sstream>
#include <algorithm>
#include <json/json.h>

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

void load_direct_index(const std::string &filename, std::vector<DirectIndex> &direct_index) {
    std::ifstream infile(filename, std::ios::binary);
    if (!infile) {
        std::cerr << "Не удалось открыть файл прямого индекса!" << std::endl;
        return;
    }

    while (infile) {
        DirectIndex doc;
        uint64_t title_size, url_size;

        infile.read(reinterpret_cast<char*>(&title_size), sizeof(title_size));
        if (infile.eof()) break;

        doc.title.resize(title_size);
        infile.read(&doc.title[0], title_size);

        infile.read(reinterpret_cast<char*>(&url_size), sizeof(url_size));
        doc.url.resize(url_size);
        infile.read(&doc.url[0], url_size);

        uint64_t doc_id_size;
        infile.read(reinterpret_cast<char*>(&doc_id_size), sizeof(doc_id_size));
        doc.doc_id.resize(doc_id_size);
        infile.read(&doc.doc_id[0], doc_id_size);

        direct_index.push_back(doc);
    }
}

void load_inverted_index(const std::string &filename, std::vector<InvertedIndex> &inverted_index) {
    std::ifstream infile(filename, std::ios::binary);
    if (!infile) {
        std::cerr << "Не удалось открыть файл обратного индекса!" << std::endl;
        return;
    }

    while (infile) {
        InvertedIndex term;
        uint64_t term_size, doc_count;

        infile.read(reinterpret_cast<char*>(&term_size), sizeof(term_size));
        if (infile.eof()) break;

        term.term.resize(term_size);
        infile.read(&term.term[0], term_size);

        infile.read(reinterpret_cast<char*>(&doc_count), sizeof(doc_count));

        term.doc_ids.resize(doc_count);
        for (size_t i = 0; i < doc_count; ++i) {
            uint64_t doc_id_size;
            infile.read(reinterpret_cast<char*>(&doc_id_size), sizeof(doc_id_size));
            term.doc_ids[i].resize(doc_id_size);
            infile.read(&term.doc_ids[i][0], doc_id_size);
        }

        inverted_index.push_back(term);
    }
}

std::vector<std::string> parse_query(const std::string &query) {
    std::vector<std::string> tokens;
    std::string token;
    bool in_quotes = false;

    for (size_t i = 0; i < query.size(); ++i) {
        char c = query[i];

        if (c == '"' && !in_quotes) {
            in_quotes = true;
        } else if (c == '"' && in_quotes) {
            in_quotes = false;
        } else if ((c == ' ' || c == '(' || c == ')') && !in_quotes) {
            if (!token.empty()) {
                tokens.push_back(token);
                token.clear();
            }
        } else if (c == '&' || c == '|' || c == '!') {
            if (!token.empty()) {
                tokens.push_back(token);
                token.clear();
            }

            if (i + 1 < query.size() && query[i + 1] == c) {
                tokens.push_back(std::string(1, c) + c);
                ++i;
            } else {
                tokens.push_back(std::string(1, c));
            }
        } else {
            token += c;
        }
    }

    if (!token.empty()) {
        tokens.push_back(token);
    }

    return tokens;
}

std::vector<std::string> and_operation(const std::vector<std::string> &left, const std::vector<std::string> &right) {
    std::vector<std::string> result;
    if (left.empty() || right.empty()) return result;
    std::set_intersection(left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(result));
    return result;
}

std::vector<std::string> or_operation(const std::vector<std::string> &left, const std::vector<std::string> &right) {
    std::vector<std::string> result;
    if (left.empty() || right.empty()) return result;
    std::set_union(left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(result));
    return result;
}

std::vector<std::string> not_operation(const std::vector<std::string> &left, const std::vector<std::string> &right) {
    std::vector<std::string> result;
    if (left.empty()) return result;
    std::set_difference(left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(result));
    return result;
}

std::vector<std::string> boolean_search(const std::string &query, const std::vector<InvertedIndex> &inverted_index, const std::vector<DirectIndex> &direct_index) {
    auto tokens = parse_query(query);
    std::vector<std::vector<std::string>> stack;

    for (const std::string &token : tokens) {
        if (token == "&&" || token == "||" || token == "!") {
            if (stack.size() < 2) continue;

            std::vector<std::string> right = stack.back(); stack.pop_back();
            std::vector<std::string> left = stack.back(); stack.pop_back();
            std::vector<std::string> result;

            if (token == "&&") {
                result = and_operation(left, right);
            } else if (token == "||") {
                result = or_operation(left, right);
            } else if (token == "!") {
                result = not_operation(left, right);
            }

            stack.push_back(result);
        } else {
            bool found = false;
            for (const auto &entry : inverted_index) {
                if (entry.term == token) {
                    stack.push_back(entry.doc_ids);
                    found = true;
                    break;
                }
            }
            if (!found) continue;
        }
    }

    if (stack.empty()) return {};

    return stack.back();
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        std::cerr << "Не указан путь к файлу с запросами." << std::endl;
        return 1;
    }

    std::string query_file = argv[1];
    std::ifstream infile(query_file);
    if (!infile.is_open()) {
        std::cerr << "Не удалось открыть файл с запросами." << std::endl;
        return 1;
    }

    std::vector<DirectIndex> direct_index;
    std::vector<InvertedIndex> inverted_index;
    load_direct_index("data/direct_index.bin", direct_index);
    load_inverted_index("data/inverted_index.bin", inverted_index);

    std::string query;
    while (std::getline(infile, query)) {
        if (query.empty()) continue;

        std::vector<std::string> result = boolean_search(query, inverted_index, direct_index);

        if (result.empty()) {
            std::cout << "По запросу '" << query << "' ничего не найдено." << std::endl;
        } else {
            for (const auto& doc_id : result) {
                auto it = std::find_if(direct_index.begin(), direct_index.end(), [doc_id](const DirectIndex &doc) {
                    return doc.doc_id == doc_id;
                });
                if (it != direct_index.end()) {
                    std::cout << "Документ: " << doc_id << " | Заголовок: " << it->title << " | Ссылка: " << it->url << std::endl;
                }
            }
        }
        std::cout << std::endl;
    }

    return 0;
}
