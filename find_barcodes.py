#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import util
# time python find_barcodes.py -f /Users/ashipunova/BPC/linda/1_100.sorted.txt

"""For each len from 4 to 30
find lev_dist
if lev_dist < 2
get percent of beginnings "^seq"

TODO: for all pairs with low distance get alignment and count percentage
cat 1_100.txt | green_grep -e "^TGGGGAATATTG[AC]"

"""
from collections import defaultdict
import argparse
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import difflib

class Plots:
  def __init__(self, distances_dict_arr):
    self.distances_dict_arr = distances_dict_arr
    """defaultdict(None, {'len': 29, 'dist': 2.0, 'seq1': 'TGGGGAATATTGCACAATGGGGGAAACCC', 'seq2': 'TAGGGAATATTGGACAATGGGGGAAACCC', 'freq1': 1178, 'freq2': 1178})"""

    pp = PdfPages('multipage.pdf')

    for len_arr in self.distances_dict_arr:
      data = {"freq_sum": [], "dist": [], "label": []}
      for curr_dict in self.distances_dict_arr[len_arr]:
        """defaultdict(<class 'dict'>, {'len': 13, 'dist': 1.0, 'freq_sum': 236489})"""
        # if curr_dict["dist"] < 3:
        data["freq_sum"].append(curr_dict["freq_sum"])
        data["dist"].append(curr_dict["dist"])
        curr_seq = list({curr_dict["seq1"], curr_dict["seq2"]})
        data["label"].append(", ".join(curr_seq))

      plt.figure(figsize = (10, 8))
      plt.title(len_arr, fontsize = 20)
      plt.xlabel('freq_sum', fontsize = 15)
      plt.ylabel("dist", fontsize = 15)

      this_label = ", ".join(list(set([data["label"][i] for i, fr in enumerate(data["freq_sum"]) if fr > 140000])))
      print("this_label = %s" % this_label)
      plt.axvline(140000, color = "red", label = 'Seq with freq > then here: {}'.format(this_label))
      # plt.scatter(data["freq_sum"], data["dist"], marker = 'o')
      plt.plot(data["freq_sum"], data["dist"])
      plt.legend(loc = 0)

      # add labels

      if len_arr == 5:
        plt.show()
      plt.savefig(pp, format = 'pdf')

    pp.close()

  def add_labels(self, plt, data):
    for label, x, y in zip(data["label"], data["freq_sum"], data["dist"]):
      plt.annotate(label, xy = (x, y))
    return plt


class Sequences:
  def __init__(self, seq_file):
    f = open(seq_file, 'r')
    infile_text = f.readlines()
    self.all_seq = []
    self.collect_data(infile_text)

    self.min_freq = 2000
    self.start_length = 4
    self.end_length = 35

    # self.all_freq = self.find_freq()
    self.sum_freq = self.get_sum_freq()

    self.distances = []
    self.find_dist()
    self.freq_dist_dict = defaultdict(list)
    perc_counter = self.analyse_dist()
    self.get_percent(perc_counter)

  def collect_data(self, infile_text):
    for l in infile_text:
      line_items = l.strip().split()
      curr_dict = defaultdict()
      curr_dict["freq"] = int(line_items[0])
      curr_dict["seq"] = line_items[1]

      self.all_seq.append(curr_dict)

  def find_freq(self):
    return [d["freq"] for d in self.all_seq]

  def get_sum_freq(self):
    return sum([d["freq"] for d in self.all_seq])

  # TODO: add sliding window to remove random 4 nd?
  def find_dist(self):
    reversed_fr_seq_d_arr = self.all_seq[::-1]
    for i, d in enumerate(reversed_fr_seq_d_arr):
      full_seq1 = reversed_fr_seq_d_arr[i]["seq"]
      try:
        full_seq2 = reversed_fr_seq_d_arr[i + 1]["seq"]
        freq1 = reversed_fr_seq_d_arr[i]["freq"]
        freq2 = reversed_fr_seq_d_arr[i + 1]["freq"]

        if (not freq1 < self.min_freq) and (not freq2 < self.min_freq):
          # end_length = max(len(full_seq1), len(full_seq2))

          for l in range(self.start_length, self.end_length):
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
    return matrix[size_x - 1, size_y - 1]

  def get_seq_low_dist_dist(self):
    for d in self.distances:
      if d["dist"] > 0:
        text = """len = %d, seq1 %s and seq2 %s has distance %d with freq1 %f, freq2 %f""" % (
          d["len"], d["seq1"], d["seq2"], d["dist"], d["freq1"], d["freq2"])
        print(text)

  def get_all_seq_good_dist(self):
    # def custom_key(in_str):
    #   return len(in_str), in_str.lower()

    all_seq_good_dist = set()
    all_seq_good_dist_list = []
    for d in self.distances:
      # defaultdict(None, {'freq1': 165648, 'freq2': 70841, 'len': 4, 'seq1': 'TGGG', 'seq2': 'TGGG', 'dist': 0.0})
      if d["dist"] < 2:
        all_seq_good_dist.add(d["seq1"])
        all_seq_good_dist.add(d["seq2"])
        all_seq_good_dist_list = sorted(all_seq_good_dist)
        all_seq_good_dist_list.sort(key = len)

    return all_seq_good_dist_list
    # sorted(all_seq_good_dist, key=custom_key)

  def get_all_seq_good_dist_w_align(self):
    all_seq_good_dist = set()
    all_seq_good_dist_list = []
    for d in self.distances:
      # defaultdict(None, {'freq1': 165648, 'freq2': 70841, 'len': 4, 'seq1': 'TGGG', 'seq2': 'TGGG', 'dist': 0.0})
      if d["dist"] < 2:
        if d["seq1"] != d["seq2"]:
          aligned_seq = self.align(d["seq1"], d["seq2"])
          all_seq_good_dist.add(aligned_seq)
        else:
          all_seq_good_dist.add(d["seq1"])

      all_seq_good_dist_list = sorted(all_seq_good_dist)
      all_seq_good_dist_list.sort(key = len)

    return all_seq_good_dist_list
    # sorted(all_seq_good_dist, key=custom_key)


  def analyse_dist(self):
    perc_dict = defaultdict()
    all_seq_good_dist_list = self.get_all_seq_good_dist_w_align()
    for curr_seq in all_seq_good_dist_list:
      cntr = 0
      for e_dict in self.all_seq:
        if e_dict["seq"].startswith(curr_seq):
          cntr = cntr + e_dict["freq"]
      perc_dict[curr_seq] = cntr
    return perc_dict

  def get_percent(self, perc_dict):
    for seq, cnts in perc_dict.items():
      perc50 = float(self.sum_freq) / 2  # 701616
      if cnts > perc50:
        perc = 100 * cnts / float(self.sum_freq)
        print("{} {}: {:.1f}%".format(seq, cnts, round(perc, 1)))

  def align(self, a, b):
    res_seq = ""
    new_group = []

    # print('{} => {}'.format(a, b))
    for i, s in enumerate(difflib.ndiff(a, b)):
      if s[0] == ' ':
        if len(new_group) > 0:
          res_seq += "[{}]".format("".join(new_group))
        res_seq += s[-1]
        new_group = []
      elif s[0] == '-':
        new_group.append(s[-1])
      elif s[0] == '+':
        new_group.append(s[-1])
    if len(new_group) > 0:
      res_seq += "[{}]".format("".join(new_group))
    return res_seq


if __name__ == '__main__':

  parser = argparse.ArgumentParser()

  """
    make file:
    for file in SRR*_1.fastq; do cat $file | grep -A1 "^@S"| grep -v "^@S"| grep -v "\-\-" | cut -c1-50 >>~/1_50.Hoellein2014_SRP042298.txt; done
    time cat ~/1_50.Hoellein2014_SRP042298.txt | sort | uniq -c | sort -n >~/1_50.Hoellein2014_SRP042298.sorted.uniqed.txt
  
    file format:  
         1 AAACGAATCTTACGCAAAGGGCGAAAGCCTGAGGGAGCAATGCAGCGTGAGGGAAGAAGCATTATCGATGTGTAAACACCTGACAGGGGCTATGAATACT
    ...  
     70841 TGGGGAATATTGGACAATGGGGGCAACCCTGATCCAGCCATGCCGCGTGTGTGAAGAAGGCCTTCGGGTTGTAAAGCACTTTCAGTTGTGAGGAAGGGGA
    165648 TGGGGAATATTGCACAATGGGGGAAACCCTGATGCAGCCATGCCGCGTGTGTGAAGAAGGCCTTCGGGTTGTAAAGCACTTTCAGTTGTGAGGAAAAGTT"""
  parser.add_argument('-f', '--file_name',
                      required = True, action = 'store', dest = 'input_file',
                      help = '''Input file name''')
  parser.add_argument("-ve", "--verbatim",
                      required = False, action = "store_true", dest = "is_verbatim",
                      help = """Print an additional information""")

  args = parser.parse_args()

  is_verbatim = args.is_verbatim

  sequences = Sequences(args.input_file)
  # plots = Plots(sequences.freq_dist_dict)
  # sequences.get_seq_low_dist_dist()

  if is_verbatim:
    print('args = ')
    print(args)
