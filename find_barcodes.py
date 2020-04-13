#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import util
# time python find_barcodes.py -f /Users/ashipunova/BPC/linda/1_100.sorted.txt

"""For each len from 4 to 30
find lev_dist
if lev_dist < 2
get percent of beginnings "^seq"
cat 1_100.txt | green_grep -e "^TGGGGAATATTG[AC]"

TODO: add tests
"""
from collections import defaultdict
import argparse
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import difflib
import re


class Sequences:
  def __init__(self, args):

    # defaults:
    self.min_freq = int(args.min_freq) or 2000
    self.start_length = int(args.start_length) or 4
    self.end_length = int(args.end_length) or 35
    self.min_perc = int(args.min_perc) or 70
    self.max_distance = int(args.max_distance) or 2
    self.sort_by_percent = args.sort_by_percent or True

    self.input_file = args.input_file
    self.all_seq = []
    self.total_seq = 0
    self.distances = []
    self.perc_dict = defaultdict()
    self.output_list = []
    self.output_text = ""

  def run_analysis(self):
    self.get_input_data()
    self.total_seq = self.get_sum_freq()
    self.test_seq_amount_and_find_dist()
    self.analyse_dist_w_re()
    self.get_percent()
    self.print_output()

  def get_input_data(self):
    f = open(self.input_file, 'r')
    infile_text = f.readlines()
    self.parse_data(infile_text)

  def parse_data(self, infile_text):
    for l in infile_text:
      line_items = l.strip().split()
      curr_dict = defaultdict()
      curr_dict["freq"] = int(line_items[0])
      curr_dict["seq"] = line_items[1]

      self.all_seq.append(curr_dict)

  def get_sum_freq(self):
    return sum([d["freq"] for d in self.all_seq])

  def test_seq_amount_and_find_dist(self):
    if len(self.all_seq) == 1:
      print("There is only one entry: {}. That means all the sequences have the same beginning.".format(
        self.all_seq[0]["seq"]))
    else:
      self.find_dist()

  def find_dist(self):
      reversed_fr_seq_d_arr = self.all_seq[::-1]
      for i, d in enumerate(reversed_fr_seq_d_arr):
        full_seq1 = reversed_fr_seq_d_arr[i]["seq"]
        try:
          full_seq2 = reversed_fr_seq_d_arr[i + 1]["seq"]
          freq1 = reversed_fr_seq_d_arr[i]["freq"]
          freq2 = reversed_fr_seq_d_arr[i + 1]["freq"]

          if (not freq1 < self.min_freq) and (not freq2 < self.min_freq):
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
        except IndexError:
          pass
        except:
          raise

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
    # used for debugging
    for d in self.distances:
      if d["dist"] > 0:
        text = """len = %d, seq1 %s and seq2 %s has distance %d with freq1 %f, freq2 %f""" % (
          d["len"], d["seq1"], d["seq2"], d["dist"], d["freq1"], d["freq2"])
        print(text)

  def get_all_seq_good_dist_w_align(self):
    all_seq_good_dist = set()
    all_seq_good_dist_list = []
    for d in self.distances:
      # defaultdict(None, {'freq1': 165648, 'freq2': 70841, 'len': 4, 'seq1': 'TGGG', 'seq2': 'TGGG', 'dist': 0.0})
      if d["dist"] < self.max_distance:
        if d["seq1"] != d["seq2"]:          
          aligned_seq = self.align(d["seq1"], d["seq2"])
          all_seq_good_dist.add(aligned_seq)
        else:
          all_seq_good_dist.add(d["seq1"])

      all_seq_good_dist_list = sorted(all_seq_good_dist)
      all_seq_good_dist_list.sort(key = len)

    return all_seq_good_dist_list

  def analyse_dist_w_re(self):
    all_seq_good_dist_list = self.get_all_seq_good_dist_w_align()
    for curr_seq in all_seq_good_dist_list:
      cntr = 0
      for e_dict in self.all_seq:
        if e_dict["seq"].startswith(curr_seq) or re.search("^" + curr_seq, e_dict["seq"]):
          cntr = cntr + e_dict["freq"]
      self.perc_dict[curr_seq] = cntr

  def get_all_seq_good_dist(self):
    """Use for exact match only, no alignment. It is faster"""
    # def custom_key(in_str):
    #   return len(in_str), in_str.lower()

    all_seq_good_dist = set()
    all_seq_good_dist_list = []
    for d in self.distances:
      # defaultdict(None, {'freq1': 165648, 'freq2': 70841, 'len': 4, 'seq1': 'TGGG', 'seq2': 'TGGG', 'dist': 0.0})
      if d["dist"] < self.max_distance:
        all_seq_good_dist.add(d["seq1"])
        all_seq_good_dist.add(d["seq2"])
        all_seq_good_dist_list = sorted(all_seq_good_dist)
        all_seq_good_dist_list.sort(key = len)

    return all_seq_good_dist_list
    # sorted(all_seq_good_dist, key=custom_key)

  def analyse_dist(self):
    """Use for exact match only, no alignment. It is faster"""
    perc_dict = defaultdict()
    all_seq_good_dist_list = self.get_all_seq_good_dist()
    for curr_seq in all_seq_good_dist_list:
      cntr = 0
      for e_dict in self.all_seq:
        if e_dict["seq"].startswith(curr_seq):
          cntr = cntr + e_dict["freq"]
      perc_dict[curr_seq] = cntr
    return perc_dict

  def get_percent(self):
    perc50 = float(self.total_seq) / 2
    for seq, cnts in self.perc_dict.items():
      # only if more then 50%
      if cnts > perc50:
        perc = 100 * cnts / float(self.total_seq)
        # only if more then our threshold
        if perc > self.min_perc:
          self.output_list.append((seq, cnts, perc))
          # print("{} {}: {:.1f}%".format(seq, cnts, round(perc, 1)))

  def print_output(self):
    curr_output_list = self.output_list
    if self.sort_by_percent:
      curr_output_list = sorted(self.sort_by_sub_list(self.output_list), key = len)

    self.output_text = "\n".join(["{} {}: {:.1f}%".format(e[0], e[1], round(e[2], 1)) for e in curr_output_list])
    print(self.output_text)

  def sort_by_sub_list(self, sub_li):
    # reverse = True (Sorts in Descending order)
    # key is set to sort using third element of sub_li (percentage)
    sub_li.sort(key = lambda x: x[2], reverse = True)
    return sub_li

  def make_new_group_str(self, new_gr_l):
    return "[{}]".format("".join(sorted(new_gr_l)))

  def align(self, a, b):
    # TODO: 'TG[A]G[G]' - ?
    res_seq = ""
    new_group = []

    # print('{} => {}'.format(a, b))
    for i, s in enumerate(difflib.ndiff(a, b)):
      if s[0] == ' ':
        if len(new_group) > 0:
          res_seq += self.make_new_group_str(new_group)
        res_seq += s[-1]
        new_group = []
      elif s[0] == '-':
        new_group.append(s[-1])
      elif s[0] == '+':
        new_group.append(s[-1])
    if len(new_group) > 0:
      res_seq += self.make_new_group_str(new_group)
    return res_seq


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
      plt.axvline(140000, color = "red", label = 'Seq with freq > than here: {}'.format(this_label))
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


class Tests:
  def __init__(self):
    self.input = ['   5382 TGGGGAATATTGGACAATGGGGGCAACCCTGATCCAGCCATGCCGCGTGTGTGAAGAAGGCCTTCGGGTTGTAAAGCACTTTCAGTTGTGAGGAAGGGGT\n', '   5776 TGAGGAATATTGGGCAATGGAGGCAACTCTGACCCAGCCATGCCGCGTGCAGGAAGACTGCCCTATGGGTTGTAAACTGCTTTTATACGGGAAGAAACAC\n', '   6105 TGAGGAATATTGGACAATGGGCGGGAGCCTGATCCAGCCATGCCGCGTGCAGGATGACGGCCCTATGGGTTGTAAACTGCTTTTATACGGGAAGAAACGC\n', '   6108 TGGGGAATATTGGACAATGGGCGCAAGCCTGATCCAGCCATGCCGCGTGTGTGATGAAGGCCCTAGGGTTGTAAAGCACTTTCAACGGTGAAGATAATGA\n', '   6669 TGAGGAATCTTGGACAATGGGCGAAAGCCTGATCCAGCCATGCCGCGTGAATGATGAAGGCCTTAGGGTTGTAAAATTCTTTCAGCAGGGAAGATAATGA\n', '   7195 TAGGGAATATTGGTCAATGGGCGAGAGCCTGAACCAGCCATGCCGCGTGCAGGAAGACGGCCTTCTGGGTTGTAAACTGCTTTTATCAGGGAACAAAAAG\n', '   7645 TGGGGAATCTTAGACAATGGGCGCAAGCCTGATCTAGCCATGCCGCGTGAGTGACGAAGGCCTTAGGGTCGTAAAGCTCTTTCGCCTGTGATGATAATGA\n', '   8548 TGAGGAATATTGGACAATGGTCGCAAGACTGATCCAGCCATGCCGCGTGCAGGAAGACTGCCCTATGGGTTGTAAACTGCTTTTATATGGGAAGAATAAG\n', '   8807 TGGGGAATCTTGGACAATGGGGGAAACCCTGATCCAGCCATGCCGCGTGAGTGATGAAGGCCTTAGGGTCGTAAAGCTCTTTCAGCTGGGAAGATAATGA\n', '  10321 TAAGGAATATTGGACAATGGGCGCAAGCCTGATCCAGCTATCCCGCGTGCAGGATGACGGCCCTATGGGTTGTAAACTGCTTTTGTACAGGAAGAAACGC\n', '  10465 TCGAGAATCTTCTGCAATGAACGCAAGTTTGACAGAGCGACGCCGCGTGTAGGATTGAAGGCCCTTGGGTTGTAAACTACTGTTACAGGTTAAGAAATAT\n', '  10689 TCGAGAATCTTCCGCAATGGGCGAAAGCCTGACGGAGCGACACCGCGTGCAGGATGAAGGCCTTCGGGTTGTAAACTGCTGTCACGTTTCTAGGAAATGC\n', '  11251 TGGGGAATCTTGGACAATGGGGGAAACCCTGATCCAGCCATGCCGCGTGAATGATGAAGGCCTTAGGGTTGTAAAATTCTTTCAGCAGGGAAGATAATGA\n', '  11326 TGGGGAATATTGCGCAATGGGGGAAACCCTGACGCAGCCATGCCGCGTGTGTGAAGAAGGCTTTCGGGTTGTAAAGCACTTTCAATAGGGAGGAAAGGTT\n', '  11894 TAACGAATCTTCCGCAATGGGCGAAAGCCTGACGGAGCAATGCCGCGTGTGGGATGAAGCATCTTCGATGTGTAAACCACTGTCAGGGTCTAGGAATACT\n', '  13205 TGGGGAATCTTAGACAATGGGCGCAAGCCTGATCTAGCCATGCCGCGTGAGTGATGAAGGCCTTAGGGTCGTAAAACTCTTTCGCCAGGGATGATAATGA\n', '  13489 TGGGGAATATTGGACAATGGGCGCAAGCCTGATCCAGCCATGCCGCGTGAGTGATGAAGGCCTTAGGGTTGTAAAGCTCTTTCGCCGGGGAAGATAATGA\n', '  13752 TGGGGAATCTTGGACAATGGGCGCAAGCCTGATCCAGCCATGCCGCGTGAGTGATGAAGGCCTTAGGGTCGTAAAGCTCTTTCGCCTGTGATGATAATGA\n', '  22521 TGGGGAATATTGGGCAATGGGCGCAAGCCTGACCCAGCCATGCCGCGTGTGTGAAGAAGGCTTTCGGGTTGTAAAGCACTTTAAGTTGGGAGGAAGGCTG\n', '  25220 TGGGGAATATTGCACAATGGGCGCAAGCCTGATGCAGCCATGCCGCGTGTGTGATGAAGGCCTTAGGGTTGTAAAACACTTTCATCGGTGAAGATAATGA\n', '  25620 TGGGGAATATTGGACAATGGGGGCAACCCTGATCCAGCCATGCCGCGTGTGTGAAGAAGGCTTTCGGGTTGTAAAGCACTTTCAGTGAGGAGGAAAAGTT\n', '  28544 TGGGGAATATTGGACAATGGGCGAAAGCCTGATCCAGCAATTCCGCGTGTGTGAAGAAGGCCTTAGGGTTGTAAAGCACTTTAGTTCGGGAAGAAAAAGC\n', '  31834 TGGGGAATATTGGACAATGGGGGCAACCCTGATCCAGCCATGCCGCGTGTGTGAAGAAGGCTTTCGGGTTGTAAAGCACTTTCAGTGAGGAGGAAAACCT\n', '  32941 TGGGGAATCTTGGACAATGGGGGCAACCCTGATCCAGCCATGCCGCGTGTGTGAAGAAGGCCTTCGGGTTGTAAAGCACTTTCAGTAGGGAGGAAGGCTT\n', '  37331 TGGGGAATCTTGGACAATGGGGGCAACCCTGATCCAGCCATGCCGCGTGTGTGAAGAAGGCCTTCGGGTTGTAAAGCACTTTCAGCAGGGAGGAAGGCTT\n', '  44645 TGGGGAATCTTAGACAATGGGCGCAAGCCTGATCTAGCGATGCCGCGTGAGTGATGAAGGCCTTAGGGTCGTAAAGCTCTTTCGCCTGTGAAGATAATGA\n', '  50525 TGGGGAATCTTGCACAATGGGCGAAAGCCTGATGCAGCCATGCCGCGTGAATGATGAAGGCCTTAGGGTTGTAAAATTCTTTCGCTAGGGATGATAATGA\n', '  69978 TGGGGAATATTGGACAATGGGGGCAACCCTGATCCAGCCATGCCGCGTGAGTGAAGAAGGCCTTCGGGTTGTAAAGCTCTTTCAGATGCGAAGATGATGA\n', '  70841 TGGGGAATATTGGACAATGGGGGCAACCCTGATCCAGCCATGCCGCGTGTGTGAAGAAGGCCTTCGGGTTGTAAAGCACTTTCAGTTGTGAGGAAGGGGA\n', ' 165648 TGGGGAATATTGCACAATGGGGGAAACCCTGATGCAGCCATGCCGCGTGTGTGAAGAAGGCCTTCGGGTTGTAAAGCACTTTCAGTTGTGAGGAAAAGTT\n']

    self.output = """TG[AG]GGAA 723711: 93.5%
TGGGGAA 696613: 90.0%
T[AG]GGGAA 703808: 90.9%
TGGGGAAT[AC]TTG 631118: 81.5%
TG[AG]GGAAT 723711: 93.5%
TGGGGAAT 696613: 90.0%
TGGGGA 696613: 90.0%
TGGGGAAT[AC]T 696613: 90.0%
TGGG 696613: 90.0%
T[AG]GGGAAT 703808: 90.9%
TGGGGAAT[AC] 696613: 90.0%
TGGGGAAT[AC]TT 696613: 90.0%
TG[AG]GGA 723711: 93.5%
TGGGG 696613: 90.0%
T[AG]GGGA 703808: 90.9%"""

  def test_res(self):
    test_sequences = Sequences(args)

    test_sequences.sort_by_percent = False

    test_sequences.parse_data(self.input)
    test_sequences.total_seq = test_sequences.get_sum_freq()

    test_sequences.test_seq_amount_and_find_dist()

    test_sequences.analyse_dist_w_re()
    test_sequences.get_percent()
    print("===== Test output =====")
    test_sequences.print_output()

    assert test_sequences.output_text == self.output, "{} should be {}".format(test_sequences.output_text, self.output)


class Usage:
  def __init__(self):
    self.args = self.parse_args()
    self.is_verbatim = self.args.is_verbatim

  def parse_args(self):
    myusage = """python %(prog)s -f FILENAME [optional parameters]
  
      Prints out 'beginnings' with its total occurrence and percentage.
    ==========
    Input file format:
             1 AAACGAATCTTACGCAAAGGGCGAAAGCCTGAGGGAGCAATGCAGCGTGAGGGAAGAAGCATTATCGATGTGTAAACACCTGACAGGGGCTATGAATACT
        ...  
         70841 TGGGGAATATTGGACAATGGGGGCAACCCTGATCCAGCCATGCCGCGTGTGTGAAGAAGGCCTTCGGGTTGTAAAGCACTTTCAGTTGTGAGGAAGGGGA
        165648 TGGGGAATATTGCACAATGGGGGAAACCCTGATGCAGCCATGCCGCGTGTGTGAAGAAGGCCTTCGGGTTGTAAAGCACTTTCAGTTGTGAGGAAAAGTT
   
    Usage example:
    1) First look at the file names and the header line and change "*_R1.fastq" and "^@M" accordingly in the command line below. 
    2) The second line will print out sequences only, each cut down to the first 50 nd. 
    3) If sequences have first X random nucleotides change "cut -c1-50" to "cut -cX-50" in the bash command above.
    
    for file in *_R1.fastq; 
    do cat $file | grep -A1 "^@M" | grep -v "^@M" | grep -v "\-\-" | cut -c1-50 >>~/1_50.txt; 
    done
  
    time cat ~/1_50.txt | sort | uniq -c | sort -n >~/1_50.sorted.uniqued.txt
  
    time python find_barcodes.py -f ~/1_50.sorted.uniqued.txt
  
    Usage example for each file separately (useful if each file has a different adapter in front of a common primer):
    cd /xraid2-2/g454/run_new_pipeline/miseq/20200227/lane_1_B/analysis
  
    for file in *R1.fastq; 
    do 
    echo $file 
    echo $file >> ~/1_50.res.txt;
    time cat $file | grep -A1 "^@M"| grep -v "^@M"| grep -v "\-\-" | cut -c1-50 | sort | uniq -c | sort -n >~/1_50.$file.sorted.uniqued.txt;
  
    time python /xraid/bioware/linux/seqinfo/bin/find_barcodes.py -f ~/1_50.$file.sorted.uniqued.txt >> ~/1_50.res.txt;
    done
    mail_done
  
    ==========
    Default thresholds:
      Do not take "beginnings" from sequences with frequency less than 2000 (min_freq)
      Filter out "beginnings" if the Levenstein distance between them is equal or greater than 2 (max_distance)
      Minimum "beginning" length = 4 (start_length)
      Maximum "beginning" length = 35 (end_length)
      Print out results if percentage is grater than 70 (min_perc)
      Results are printed sorted by percentage, the greatest percentage first (sort_by_percent)
    ==========
    Output example:
      TG[AG]GGA 1224498: 87.3%%
      TGGG 1155916: 82.4%%
      TGGGG 1150625: 82.0%%
      TGGGGA 1141747: 81.4%%
      TGGGGAA 1135249: 80.9%%
      TGGGGAAT 1131727: 80.7%%
    ==========
    """

    parser = argparse.ArgumentParser(usage = myusage)

    parser.add_argument('-f', '--file_name',
                        required = True, action = 'store', dest = 'input_file',
                        help = '''Input file name.''')
    parser.add_argument('-fr', '--min_freq',
                        required = False, action = 'store', dest = 'min_freq', default = 2000,
                        help = '''Do not take "beginnings" from sequences with frequency less than MIN_FREQ.''')
    parser.add_argument('-st', '--start_length',
                        required = False, action = 'store', dest = 'start_length', default = 4,
                        help = '''Minimum "beginning" length = START_LENGTH.''')
    parser.add_argument('-en', '--end_length',
                        required = False, action = 'store', dest = 'end_length', default = 35,
                        help = '''Maximum "beginning" length = END_LENGTH.''')
    parser.add_argument('-mp', '--min_perc',
                        required = False, action = 'store', dest = 'min_perc', default = 70,
                        help = '''Print out results if percentage is grater than MIN_PERC.''')
    parser.add_argument('-ld', '--max_distance',
                        required = False, action = 'store', dest = 'max_distance', default = 2,
                        help = '''Filter out "beginnings" if the Levenstein distance between them is equal or greater than MAX_DISTANCE. To do only the exact match comparison with no mismatches use --max_distance 1.''')
    parser.add_argument('-ps', '--sort_by_percent',
                        required = False, action = 'store_false', dest = 'sort_by_percent',
                        help = '''By default the output is sorted by percent. If this option is provided the output is printed sorted by length.''')

    parser.add_argument("-te", "--test_only",
                        required = False, action = "store_true", dest = "test_only",
                        help = """Run only the test.""")
    parser.add_argument("-ve", "--verbatim",
                        required = False, action = "store_true", dest = "is_verbatim",
                        help = """Print an additional information.""")
    if len(sys.argv) == 1:
      parser.print_help(sys.stderr)
      sys.exit(1)

    return parser.parse_args()


if __name__ == '__main__':

  usage = Usage()
  args = usage.args
  if args.is_verbatim:
    print('args = ')
    print(args)

  if args.test_only:
    tests = Tests()
    tests.test_res()
  else:
    sequences = Sequences(args)
    sequences.run_analysis()
    # tests = Tests()
    # tests.test_res()
  # plots = Plots(sequences.freq_dist_dict)
  # sequences.get_seq_low_dist_dist()a


