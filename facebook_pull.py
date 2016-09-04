import facebook
from urllib.parse import urlparse
from urllib.parse import parse_qs
from elasticsearch import Elasticsearch




def getPageDetails(page_id,graph):

	page = graph.get_object(id=page_id)
	page_name = page['name']
	try:
		page_mission = page['mission']
	except:
		page_mission = ""
	try:
		page_likes = page['likes']
	except:
		page_likes = 0
	try:
		page_about = page['about']
	except:
		page_about = ""

	page_info = {
	'page_id' : page_id,
	'page_name' : page_name,
	'page_mission' : page_mission,
	'page_likes' : page_likes,
	'page_about' : page_about
	}
	return page_info

def getPostDetails(post,graph):
	
	post_id = post['id']
		
	try:
		post_message = post['message']
	except:
		post_message = ""
	
	try:
		post_type = post['type']
	except:
		post_type = ""
	
	try:
		post_status_type = post['status_type']
	except:
		post_status_type = ""
	post_created_time = post['created_time']
	post_updated_time = post['updated_time']
	
	try:
		post_shares_count = post['shares']['count']
	except:
		post_shares_count = 0

	post_likes_count = 0
	
	try:
		post_likes_all = post['likes']
		post_likes = []		
		while 'paging' in post_likes_all and 'next' in post_likes_all['paging'] and post_likes_all['paging']['next']: 	
			post_likes_data = post_likes_all['data']			
			for like in post_likes_data:
				post_likes.append({'user_name' : like['name'], 'user_id' : like['id']})
				post_likes_count = post_likes_count + 1
			nextUrl = post_likes_all['paging']['next']
			parsed = urlparse(nextUrl)
			after = parse_qs(parsed.query)['after'][0]
			post_likes_all = graph.get_connections(id = post_id, connection_name = 'likes', after = after)
		
		post_likes_data = post_likes_all['data']
		for like in post_likes_data:
				post_likes.append({'user_name' : like['name'], 'user_id' : like['id']})
				post_likes_count = post_likes_count + 1

	except:	
		
		post_likes = []

	post_comments_count = 0

	try:
		post_comments_all = post['comments']
		post_comments = []

		while 'paging' in post_comments_all and 'next' in post_comments_all['paging'] and post_comments_all['paging']['next']:
		
			post_comments_data = post_comments_all['data']

			for comment in post_comments_data:
				post_comments.append({'user_name' : comment['from']['name'], 'user_id' : comment['from']['id'], 'message' : comment['message'], 'like_count' : comment['like_count']})
				post_comments_count = post_comments_count + 1

			nextUrl = post_comments_all['paging']['next']
			parsed = urlparse(nextUrl)
			after = parse_qs(parsed.query)['after'][0]
			post_comments_all = graph.get_connections(id = post_id, connection_name = 'comments', after = after)
	
		post_comments_data = post_comments_all['data']

		for comment in post_comments_data:
				post_comments.append({'user_name' : comment['from']['name'], 'user_id' : comment['from']['id'], 'message' : comment['message'], 'like_count' : comment['like_count']})
				post_comments_count = post_comments_count + 1

	except:
		post_comments = []

	post_body = {
	'post_id': post_id,
	'post_message' : post_message,
	'post_type' : post_type,
	'post_status_type' : post_status_type,
	'post_created_time' : post_created_time,
	'post_updated_time' : post_updated_time,
	'post_shares_count' : post_shares_count,
	'post_likes' : post_likes,
	'post_likes_count' : post_likes_count,
	'post_comments' : post_comments,
	'post_comments_count' : post_comments_count
	}

	return post_body

def indexPosts(page_id,graph,fblastpostid_old,es,brand):

	posts = graph.get_connections(id = page_id, connection_name = 'posts')
	post_check = ""
	post_counter = 0
	loop_exit = 0
	
	while 'paging' in posts and 'next' in posts['paging'] and posts['paging']['next']:

		if loop_exit == 1:
			break

		posts_data = posts['data']
		
		try:
			if posts_data[0]['id'] == post_check:
				break
		except:
			break
		
		for post in posts_data:
			
			if post_counter == 0:
				fblastpostid_new = post['id']
			post_counter = post_counter + 1

			post_info = getPostDetails(post,graph)

			post_upload = es.index(index = 'fbposts', doc_type = brand, id = post_info['post_id'], body = post_info)
			if post_upload['created'] == False:
				post_counter = post_counter - 1

			if post['id'] == fblastpostid_old:
				loop_exit = 1
				break	

		nextUrl = posts['paging']['next']
		parsed = urlparse(nextUrl)
		until = int(parse_qs(parsed.query)['until'][0])
		posts = graph.get_connections(id = page_id, connection_name = 'posts', until = until) 
		post_check = posts['data'][len(posts['data'])-1]['id']		

	print("Indexed " + str(post_counter) + " posts for " + brand)
	return fblastpostid_new


		




		    

