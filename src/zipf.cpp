#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <cstdlib>

namespace fs = std::filesystem;

struct TermFreq {
    std::string term;
    uint32_t freq;
};

static bool parse_token_from_tokens_tsv_line(const std::string& line, std::string& token_out) {
    size_t p1 = line.find('\t');
    if (p1 == std::string::npos) return false;
    size_t p2 = line.find('\t', p1 + 1);
    if (p2 == std::string::npos) return false;

    token_out = line.substr(p2 + 1);
    if (!token_out.empty() && token_out.back() == '\r') token_out.pop_back();
    return !token_out.empty();
}

static void ensure_dir(const fs::path& p) {
    std::error_code ec;
    if (!fs::exists(p, ec)) {
        fs::create_directories(p, ec);
    }
}

int main(int argc, char** argv) {
    fs::path stem_tokens = "data/tokens/tokens_stem.tsv";
    fs::path raw_tokens  = "data/tokens/tokens.tsv";
    fs::path tokens_path = fs::exists(stem_tokens) ? stem_tokens : raw_tokens;

    fs::path out_dir = (tokens_path == stem_tokens) ? fs::path("data/zipf_stem") : fs::path("data/zipf");

    int topN = 50;

    if (argc >= 2) tokens_path = argv[1];
    if (argc >= 3) out_dir = argv[2];
    if (argc >= 4) topN = std::atoi(argv[3]);

    std::cout << "Tokens file: " << tokens_path << "\n";
    std::cout << "Output dir : " << out_dir << "\n";
    std::cout << "Top-N      : " << topN << "\n";

    ensure_dir(out_dir);

    std::ifstream in(tokens_path, std::ios::binary);
    if (!in) {
        std::cerr << "ERROR: Cannot open tokens file: " << tokens_path << "\n";
        std::cerr << "Hint: run tokenizer first to generate data/tokens/tokens.tsv\n";
        return 1;
    }

    std::vector<std::string> terms;
    terms.reserve(1'000'000);

    uint64_t bytes_read = 0;
    uint64_t lines = 0;

    std::string line;
    std::string tok;

    while (std::getline(in, line)) {
        bytes_read += line.size() + 1;
        lines++;

        if (!parse_token_from_tokens_tsv_line(line, tok)) continue;
        terms.push_back(tok);
    }
    in.close();

    if (terms.empty()) {
        std::cerr << "ERROR: No terms found in tokens.tsv\n";
        return 1;
    }

    std::cout << "Read lines : " << lines << "\n";
    std::cout << "Read bytes : " << bytes_read << "\n";
    std::cout << "Tokens     : " << terms.size() << "\n";

    std::sort(terms.begin(), terms.end());

    std::vector<TermFreq> freqs;
    freqs.reserve(terms.size() / 10);

    uint32_t cur_count = 1;
    std::string cur_term = terms[0];

    for (size_t i = 1; i < terms.size(); i++) {
        if (terms[i] == cur_term) {
            cur_count++;
        } else {
            freqs.push_back({cur_term, cur_count});
            cur_term = terms[i];
            cur_count = 1;
        }
    }
    freqs.push_back({cur_term, cur_count});

    std::vector<std::string>().swap(terms);

    std::sort(freqs.begin(), freqs.end(), [](const TermFreq& a, const TermFreq& b) {
        if (a.freq != b.freq) return a.freq > b.freq;
        return a.term < b.term;
    });

    const uint32_t C = freqs[0].freq;
    const size_t V = freqs.size();

    std::cout << "Vocabulary (unique terms): " << V << "\n";
    std::cout << "C (freq at rank 1): " << C << "\n";

    fs::path out_rank = out_dir / "zipf_rank_freq.tsv";
    std::ofstream out1(out_rank, std::ios::binary);
    if (!out1) {
        std::cerr << "ERROR: Cannot write: " << out_rank << "\n";
        return 1;
    }

    out1 << "rank\tfreq\tlog10_rank\tlog10_freq\tzipf_freq\n";
    for (size_t i = 0; i < V; i++) {
        uint64_t rank = i + 1;
        uint32_t f = freqs[i].freq;

        double lr = std::log10((double)rank);
        double lf = std::log10((double)f);
        double zipf_f = (double)C / (double)rank;

        out1 << rank << "\t" << f << "\t" << lr << "\t" << lf << "\t" << zipf_f << "\n";
    }
    out1.close();

    fs::path out_top = out_dir / "zipf_terms_top.tsv";
    std::ofstream out2(out_top, std::ios::binary);
    if (!out2) {
        std::cerr << "ERROR: Cannot write: " << out_top << "\n";
        return 1;
    }

    out2 << "rank\tterm\tfreq\n";
    for (int i = 0; i < topN && (size_t)i < V; i++) {
        out2 << (i + 1) << "\t" << freqs[i].term << "\t" << freqs[i].freq << "\n";
    }
    out2.close();

    std::cout << "Saved: " << out_rank << "\n";
    std::cout << "Saved: " << out_top << "\n";
    return 0;
}
