import import_statements as ims
import set_paths as path
import os
import numpy as np
import get_hist_processing as hp
import pandas as pd

class Sqb:
	def __init__(self, df, col, string):

		self.qy = df[col].str.upper().str.contains((string))
		self.qn = (~df[col].str.upper().str.contains((string)))
		self.info = "Queries a \"Log\" \"DataFrame\" for the string "+"\""+string+ "\"" +" in the column " + "\""+str(col)+"\""


class Chain:
	def __init__(self, list_of_qs, ch_type):


		def chain_or(self):

			base = self.q_list[0]

			for query in self.q_list[1:]:
				base = (base | query)
			
			base = (base)
			return base


		def chain_and(self):
			base = self.q_list[0]
			for query in self.q_list[1:]:
				base = base & query
				base = (base)
			return base



		self.q_list = list_of_qs
		self.ch_type = ch_type
		self.chain_or = chain_or(self)
		self.chain_and = chain_and(self)


