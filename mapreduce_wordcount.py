import re
import requests
import threading
import json
from collections import defaultdict, Counter
import matplotlib.pyplot as plt

# Constants
TEXT_URL = "https://www.gutenberg.org/files/1342/1342-0.txt"
TOP_N = 10
NUM_THREADS = 4
PLOT_FILE = "top_words.png"
TXT_FILE = "top_words.txt"
JSON_FILE = "top_words.json"

def download_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Failed to download text: {e}")
        return ""

def map_words(text_chunk):
    words = re.findall(r'\b\w+\b', text_chunk.lower())
    return Counter(words)

def shuffle(mapped_data):
    grouped = defaultdict(list)
    for part in mapped_data:
        for word, count in part.items():
            grouped[word].append(count)
    return grouped

def reduce(grouped_data):
    return {word: sum(counts) for word, counts in grouped_data.items()}

def visualize_top_words(word_counts, top_n=10, filename=None):
    top = Counter(word_counts).most_common(top_n)
    if not top:
        print("No data to visualize.")
        return

    words, counts = zip(*top)
    plt.figure(figsize=(10, 6))
    plt.bar(words, counts, color="skyblue")
    plt.title(f"Top {top_n} Most Frequent Words")
    plt.ylabel("Frequency")
    plt.xticks(rotation=45)
    plt.tight_layout()
    if filename:
        plt.savefig(filename)
        print(f"Plot saved to '{filename}'")
    plt.show()

    return top

def parallel_map(text, num_threads=4):
    length = len(text)
    if length == 0:
        return []

    chunk_size = length // num_threads
    threads = []
    results = [None] * num_threads

    def job(i, chunk):
        results[i] = map_words(chunk)

    for i in range(num_threads):
        start = i * chunk_size
        end = None if i == num_threads - 1 else (i + 1) * chunk_size
        thread = threading.Thread(target=job, args=(i, text[start:end]))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return results

def save_results_txt(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        for word, count in data:
            f.write(f"{word}: {count}\n")
    print(f"Top words saved to '{filename}'")

def save_results_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(dict(data), f, indent=4, ensure_ascii=False)
    print(f"Top words saved to '{filename}'")

def main():
    print("Downloading text...")
    text = download_text(TEXT_URL)
    if not text:
        return

    print("Processing with MapReduce...")
    mapped_data = parallel_map(text, NUM_THREADS)
    grouped_data = shuffle(mapped_data)
    reduced_data = reduce(grouped_data)

    print("Visualizing and saving results...")
    top_words = visualize_top_words(reduced_data, TOP_N, PLOT_FILE)

    if top_words:
        save_results_txt(top_words, TXT_FILE)
        save_results_json(top_words, JSON_FILE)

if __name__ == "__main__":
    main()

