import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("data/zipf/zipf_rank_freq.tsv", sep="\t")
plt.figure()
plt.plot(df["log10_rank"], df["log10_freq"], label="Corpus")
plt.plot(df["log10_rank"], (df["zipf_freq"]).apply(lambda x: __import__("math").log10(x)), label="Zipf C/r")
plt.xlabel("log10(rank)")
plt.ylabel("log10(freq)")
plt.legend()
plt.title("Zipf law (log-log)")
plt.show()

df = pd.read_csv("data/zipf_stem/zipf_rank_freq.tsv", sep="\t")
plt.figure()
plt.plot(df["log10_rank"], df["log10_freq"], label="Corpus")
plt.plot(df["log10_rank"], (df["zipf_freq"]).apply(lambda x: __import__("math").log10(x)), label="Zipf C/r")
plt.xlabel("log10(rank)")
plt.ylabel("log10(freq)")
plt.legend()
plt.title("Zipf_stem law (log-log)")
plt.show()