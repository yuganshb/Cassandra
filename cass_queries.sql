create table twitter_data( 
		quote_count int, 
		reply_count int, 
		hashtags list<text>, 
		datetime timestamp, 
		date date, 
		like_count int, 
		verified text, 
		sentiment int, 
		author text, 
		location text, 
		tid text, 
		retweet_count int, 
		type text, 
		media_list list<text>, 
		quoted_source_id text, 
		url_list list<text>, 
		tweet_text text, 
		author_profile_image text, 
		author_screen_name text, 
		author_id text, 
		lang text, 
		keywords_processed_list list<text>, 
		retweet_source_id text, 
		mentions list<text>, 
		replyto_source_id text )
		
		
		
		
		
		
		
		
create table twitter_data(quote_count int, reply_count int, hashtags list<text>, datetime timestamp, date date,like_count int,verified text,sentiment int,author text,location text,tid text,retweet_count int,type text,media_list list<text>,quoted_source_id text,url_list list<text>,tweet_text text,author_profile_image text,author_screen_name text,author_id text,lang text,keywords_processed_list list<text>,retweet_source_id text,mentions list<text>,replyto_source_id text);





select tid,tweet_text,author_id,location,lang from twitter_data where author = 'PRG' order by datetime;
