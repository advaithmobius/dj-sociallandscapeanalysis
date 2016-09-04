import csv

def extractHeader(reader,writer):
	rownum = 0
	for row in reader:
		if rownum == 0:
			colnum = 0
			for column in row:
				if column == "Brand":
					brand_colnum = colnum
				elif column == "Model":
					model_colnum = colnum
				elif column == "Location":
					location_colnum = colnum
				elif column == "Facebook":
					facebook_colnum = colnum
				elif column == "Twitter":
					twitter_colnum = colnum
				elif column == "Instagram":
					instagram_colnum = colnum
				elif column == "FbLastPostId":
					fblastpostid_colnum = colnum
				colnum = colnum + 1
			rownum = rownum + 1
			writer.writerow(row)
			break
	return {'brand_colnum':brand_colnum,'model_colnum':model_colnum,'location_colnum':location_colnum,'facebook_colnum':facebook_colnum,'twitter_colnum':twitter_colnum,'instagram_colnum':instagram_colnum,'fblastpostid_colnum':fblastpostid_colnum}