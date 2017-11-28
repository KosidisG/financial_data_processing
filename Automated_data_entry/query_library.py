import log_queries as lq


class Boc_queries:

	def __init__(self, df):

		#Strings that place the transaction CP MID Bank of Cyprus
		string_list_det = ["STAMP DUTY", 
							"OUR[\s\\n]REF", 
							"STATEMENT PRINTING",
							"IBU-MAINTENANCE FEES", 
							"IBU-LOW", "CARD MEMBERSHIP", 
							"COMMISSION/FEE",
							"CASH ADVANCE", 
							"CHEQUEBOOK ORDER"]

		string_list_lazy_det = [str.upper("DIGIPASS"), 
								str.upper("Pin Re-Print Fee"),
								str.upper("Bank services, fee"),
								str.upper("Interest expense")]


		string_list_acc_fee = ["IBU",
								"PRINT",
								"MEMBERSHIP",
								"CHEQUEBOOK",
								"DIGIPASS",
								str.upper("Bank services, fee")]

		def initial_queries(self):

			query_list = []

			for string in string_list_det:
				query_list.append(lq.Sqb(self.df, "Details", string))

			return query_list

		def lazy_query_list(self):

			query_lazy_list = []

			for i, item in enumerate(string_list_lazy_det):
				query_lazy_list.append(lq.Sqb(self.df, "Details", item).qy)

			return query_lazy_list

		def cp_boc(self):
			query_lazy_list = self.query_lazy_list

			query_list = self.query_list

			query_lazy = lq.Chain(query_lazy_list, "Or").chain_or

			boc_query_i = lq.Chain([query_list[1].qy, query_list[0].qn], "And").chain_and

			boc_query = lq.Chain([boc_query_i,  query_list[2].qy,  query_list[3].qy,  query_list[4].qy, query_list[5].qy, query_list[6].qy, query_list[7].qy, query_list[8].qy, query_lazy], "Or").chain_or

			return boc_query

		def boc_acc_fee(self):

			boc_query = self.boc_query

			acc_fees_query_list = []

			for string in string_list_acc_fee:
				acc_fees_query_list.append(lq.Sqb(self.df, "Details", string).qy)

			acc_fee_q = lq.Chain(acc_fees_query_list, "Or").chain_or

			boc_acc_fee = lq.Chain([boc_query, acc_fee_q], "And").chain_and

			return boc_acc_fee

		def interest_expense_query(self):
			string = "INTEREST EXPENSE"
			int_expn_q = lq.Chain([self.boc_query, lq.Sqb(self.df, "Details", string).qy],"And").chain_and

			return int_expn_q

		def payment_fees_query(self):
			"""This function defines the payment fees catagory by way of elimination 
			of everything that is not a payment fee"""
			payment_fees_q = lq.Chain([self.boc_query, ~self.boc_int_expn, ~self.boc_acc_fee_query ], "And").chain_and

			return payment_fees_q
		self.df = df
		self.query_list = initial_queries(self)
		self.query_lazy_list = lazy_query_list(self)
		self.boc_strings_used = string_list_det + string_list_lazy_det
		self.acc_fee_strings_used = string_list_acc_fee
		self.queries_objects_built = self.query_list + self.query_lazy_list 
		self.boc_query = cp_boc(self)
		self.boc_acc_fee_query = boc_acc_fee(self)
		self.boc_int_expn = interest_expense_query(self)
		self.boc_pay_fee_query = payment_fees_query(self)


class Boc_apply:
	def __init__(self, df):

		def broadcast(self):

			boc_mask = Boc_queries(self.df).boc_query

			boc_acc_fee_mask = Boc_queries(self.df).boc_acc_fee_query

			boc_pay_fee_mask = Boc_queries(self.df).boc_pay_fee_query

			boc_int_expn_mask = Boc_queries(self.df).boc_int_expn

			boc_broad = self.df

			boc_broad["CP MID"] = self.df["CP MID"].mask(boc_mask, "BANK OF CYPRUS PUBLIC COMPANY LTD")

			boc_broad["CC"] = self.df["CP MID"].mask(boc_mask, "RJ-ALL")

			boc_broad["GS Article"] = self.df["GS Article"].mask(boc_acc_fee_mask, "Commissions: Account Fees")

			boc_broad["GS Article"] = self.df["GS Article"].mask(boc_pay_fee_mask, "Commissions: Payment Fees")

			boc_broad["GS Article"] = self.df["GS Article"].mask(boc_int_expn_mask, "Interest: Expense")

			return boc_broad

		self.df = df
		self.apply_data = broadcast(self)