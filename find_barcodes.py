#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import util
# time python find_barcodes.py -f /Users/ashipunova/BPC/linda/1_100.sorted.txt

"""For each len from 4 to 30
what frequency is by lev_dist

then get sequences for the max len and min dist and max freq
"""
from collections import defaultdict
import argparse
import numpy as np
import matplotlib.pyplot as plt

class Plots:
  def __init__(self, distances_dict_arr):
    self.distances_dict_arr = distances_dict_arr
    """defaultdict(None, {'len': 29, 'dist': 2.0, 'seq1': 'TGGGGAATATTGCACAATGGGGGAAACCC', 'seq2': 'TAGGGAATATTGGACAATGGGGGAAACCC', 'freq1': 1178, 'freq2': 1178})"""
    """defaultdict(<class 'dict'>, {'len': 29, 'dist': 7.0, 'max_freq': 2044})"""
    data = {"len": [], "dist": [], "label": []}
    # for label, coord in test.items():
    for dict in self.distances_dict_arr:
      data["len"].append(dict["max_freq"])
      data["dist"].append(dict["dist"])
      data["label"].append(dict["len"])

    plt.figure(figsize = (10, 8))
    plt.title('Scatter Plot', fontsize = 20)
    plt.xlabel('len', fontsize = 15)
    plt.ylabel("dist", fontsize = 15)
    plt.scatter(data["len"], data["dist"], marker = 'o')

    # add labels
    # for label, x, y in zip(data["label"], data["len"], data["dist"]):
    #   plt.annotate(label, xy = (x, y))

    plt.show()


class Sequences:
  def __init__(self, seq_file):
    f = open(seq_file, 'r')
    infile_text = f.readlines()
    self.all_seq = []
    self.collect_data(infile_text)
    self.all_freq = self.find_freq()
    self.distances = []
    self.find_dist()
    # print("HERE")
    # print(self.distances)
    self.big_distances = []
    self.freq_dist_dict = defaultdict(dict)
    self.analyse_dist()

  def collect_data(self, infile_text):
    for l in infile_text:
      line_items = l.strip().split()
      curr_dict = defaultdict()
      curr_dict["freq"] = int(line_items[0])
      curr_dict["seq"] = line_items[1]

      self.all_seq.append(curr_dict)

  def find_freq(self):
    all_freq = [d["freq"] for d in self.all_seq]
    return all_freq

  def find_dist(self):
    """
      curr_dict = defaultdict()
      curr_dict["freq"] = int(line_items[0])
      curr_dict["seq"] = line_items[1]

      self.all_seq.append(curr_dict)

    """

    reversed_fr_seq_d_arr = self.all_seq[::-1]
    curr_length = 0
    for i, d in enumerate(reversed_fr_seq_d_arr):
      full_seq1 = reversed_fr_seq_d_arr[i]["seq"]
      try:
        full_seq2 = reversed_fr_seq_d_arr[i+1]["seq"]
        freq1 = reversed_fr_seq_d_arr[i]["freq"]
        freq2 = reversed_fr_seq_d_arr[i+1]["freq"]

        if (not freq1 < 1000) and (not freq2 < 1000):
          total_length = max(len(full_seq1), len(full_seq2))
          total_length = 30

          for l in range(3, total_length):
            curr_dist_dict = defaultdict()
            curr_dist_dict["freq1"] = reversed_fr_seq_d_arr[i]["freq"]
            curr_dist_dict["freq2"] = reversed_fr_seq_d_arr[i + 1]["freq"]

            seq1 = full_seq1[0:l]
            seq2 = full_seq2[0:l]

            curr_dist_dict["len"] = l
            curr_dist_dict["seq1"] = seq1
            curr_dist_dict["seq2"] = seq2

            dist = self.levenshtein(seq1, seq2)
            curr_dist_dict["dist"] = dist

            self.distances.append(curr_dist_dict)
            # print(curr_dist_dict)
      except:
        pass

  def levenshtein(self, seq1, seq2):
    size_x = len(seq1) + 1
    size_y = len(seq2) + 1
    matrix = np.zeros((size_x, size_y))
    for x in range(size_x):
      matrix[x, 0] = x
    for y in range(size_y):
      matrix[0, y] = y

    for x in range(1, size_x):
      for y in range(1, size_y):
        if seq1[x - 1] == seq2[y - 1]:
          matrix[x, y] = min(
            matrix[x - 1, y] + 1,
            matrix[x - 1, y - 1],
            matrix[x, y - 1] + 1
          )
        else:
          matrix[x, y] = min(
            matrix[x - 1, y] + 1,
            matrix[x - 1, y - 1] + 1,
            matrix[x, y - 1] + 1
          )
    # print(matrix)
    return (matrix[size_x - 1, size_y - 1])

  def analyse_dist(self):
    max_freq = 0
    for d in self.distances:
      curr_d = defaultdict(dict)
      if d["dist"] > 0:
        self.big_distances.append(d)
        freq_sum = d["freq1"] + d["freq2"]
        if max_freq < freq_sum:
          max_freq = freq_sum
        d["max_freq"] = max_freq
        """For each length get dist and freq_sum"""
        # self.freq_dist_dict[d["len"]][d["dist"]] = d
        # print(freq_dist_dict)
        curr_d["len"] = d["len"]
        curr_d["dist"] = d["dist"]
        curr_d["max_freq"] = freq_sum
        self.freq_dist_dict[d["len"]].append(curr_d)

if __name__ == '__main__':

  # utils = util.Utils()

  parser = argparse.ArgumentParser()

  parser.add_argument('-f', '--file_name',
                      required = True, action = 'store', dest = 'input_file',
                      help = '''Input file name''')
  parser.add_argument("-ve", "--verbatim",
                      required = False, action = "store_true", dest = "is_verbatim",
                      help = """Print an additional inforamtion""")

  args = parser.parse_args()
  print('args = ')
  print(args)

  is_verbatim = args.is_verbatim

  sequences = Sequences(args.input_file)
  plots = Plots(sequences.freq_dist_dict)

  # if (is_verbatim):

    # print('QQQ3 = custom_metadata_update')
    # print(custom_metadata_update)

