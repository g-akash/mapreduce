def mapper(data):
	dict = {}
	for d in data:
		if d in dict:
			dict[d] += 1
		else:
			dict[d]=1

	return dict
