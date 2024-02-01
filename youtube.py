from googleapiclient.discovery import build
import pprint
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#To connect youtube
def youtube_connect(apikey):

  api_service_name = "youtube"
  api_version = "v3"


  # Get credentials and create an API client
  youtube = build(api_service_name, api_version, developerKey=apikey)
  return youtube

youtube = youtube_connect('AIzaSyCE_Gv5g0melHEbW5XrdwBWqmiv19PG5To')

# To get channel info
def get_channeldetails(chid):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= chid
    )
    response = request.execute()
    for i in response['items']:
      channel_info = dict(channal_id = i['id'],
                          channel_name = i['snippet']['title'],
                          channel_views = i['statistics']['viewCount'],
                          channel_description = i['snippet']['description'],
                          Channel_subscribers = i['statistics']['subscriberCount'],
                          Total_vidoes = i['statistics']['videoCount'],
                          playlist_id = i['contentDetails']['relatedPlaylists']['uploads'])
    return channel_info

#to get video ids
def get_videosid(channel_name):
  response1 = youtube.channels().list(
        part="contentDetails",
        id= channel_name).execute()
  playlist = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  nextpagetoken = None
  Vidoeid = []
  while True:
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=50,
        pageToken=nextpagetoken,
        playlistId= playlist
        )
    response2 = request.execute()


    for i in range(len(response2['items'])):
      Vidoeid.append(response2['items'][i]['contentDetails']['videoId'])
    nextpagetoken = response2.get('nextPageToken')
    if nextpagetoken is None:
        break
  return Vidoeid

#to get video details
def get_videodetails(videoids):
  Video_details =[]
  for vidoeids in videoids: #getting videoID  from previos function and passing here
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id= vidoeids
      )
    response = request.execute()
    for i in response['items']:   #iteterating through each items to get all the veiode details
        details = dict(Channel_Name = i['snippet']['channelTitle'],
                        Channel_id = i['snippet']['channelId'],
                        Video_id = i['id'],
                        Title = i['snippet']['title'],
                        Tags = i['snippet'].get('tags'),
                        Thumbnail = i['snippet']['thumbnails']['default']['url'],
                        Description = i['snippet'].get('description'),
                        Published_Date = i['snippet']['publishedAt'],
                        Duration = i['contentDetails']['duration'],
                        Views = i['statistics'].get('viewCount'),
                        Likes = i['statistics'].get('likeCount'),
                        Comments = i['statistics'].get('commentCount'),
                        Favourtite_Count = i['statistics']['favoriteCount'],
                        definition = i['contentDetails']['definition'],
                        Caption_status = i['contentDetails']['caption'])
        Video_details.append(details)
  return Video_details

#To get comment details
def get_comment(videoinfo):
  comment_details=[]
  try:
    for info in videoinfo:
      request = youtube.commentThreads().list(
          part="snippet",
          videoId= info,
          maxResults=50,
          )


      response = request.execute()
      for i in response['items']:
        details = dict(Comment_id = i['id'],
                    Video_id = i['snippet']['videoId'],
                    Comment_text = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_publishdate = i['snippet']['topLevelComment']['snippet']['publishedAt'])
        comment_details.append(details)

  except:
    pass

  return comment_details

#To get playlistdetails
def get_playlist(channel_id):
  Playlistdetails = []
  nextpagetoken = None
  while True:
    request = youtube.playlists().list(
            part="snippet,contentDetails",
            maxResults=50,
            pageToken=nextpagetoken,
            channelId = channel_id
            )
    response = request.execute()
    for i in response['items']:
        details = dict(Playlist_Id = i['id'],
                        Title = i['snippet']['title'],
                        Channel_id = i['snippet']['channelId'],
                        Channel_name = i['snippet']['channelTitle'],
                        Published_at = i['snippet']['publishedAt'],
                        Video_count = i['contentDetails']['itemCount'])
        Playlistdetails.append(details)
    nextpagetoken = response.get('nextPageToken')
    if nextpagetoken is None:
      break
  return Playlistdetails

# to establish mongo Db connections
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["YoutubeData"]

# Function to insert all the youtube details in Mongo DB
def channel_info(ch_id):
  ch_details = get_channeldetails(ch_id)
  Vi_ids = get_videosid(ch_id)
  pl_details = get_playlist(ch_id)
  vi_details = get_videodetails(Vi_ids)
  comments_details = get_comment(Vi_ids)
  collection = db["channel_Details"]
  data = {"channel_information": ch_details,"playlist_details": pl_details,
          "Video_details": vi_details,"comment_info": comments_details }
  collection.insert_one(data)

  return "upload successfull"

#To create SQL tables
def channel_insert():
    mysql = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Lavani@01",
                            port="5432",
                            database="youtube_data")
    cursor = mysql.cursor()

    
    delete_query = '''drop table if exists channel''' ## this will delete the table if already exist
    cursor.execute(delete_query)
    mysql.commit()

    try: 
        create_query = '''create table if not exists channel(channal_id varchar(100) primary key,
                                                                    channel_name varchar(80),
                                                                    channel_views bigint,
                                                                    channel_description text,
                                                                    Channel_subscribers bigint,
                                                                    Total_vidoes int,
                                                                    playlist_id varchar(80))'''
        cursor.execute(create_query)
        mysql.commit()
    except:
        print("table created sucessfully")

    #   calling the mongo db and getting only channel info
    ch_list = []
    db = client["YoutubeData"]
    collection = db["channel_Details"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)


    # To get values from Dataframe and insert in to sql table

    for index,row in df.iterrows():
        insert_query = '''insert into channel(channal_id,
                                            channel_name,
                                            channel_views,
                                            channel_description,
                                            Channel_subscribers,
                                            Total_vidoes,
                                            playlist_id)  
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)''' #all are column name  from table
        values = (row['channal_id'],
                row['channel_name'],
                row['channel_views'],
                row['channel_description'],
                row['Channel_subscribers'],
                row['Total_vidoes'],
                row['playlist_id'])
                

    
        try:
            cursor.execute(insert_query,values)
            mysql.commit()
        except:
            print("channel details already inserted")

# to Insert playlist details in SQL            
def playlist_insert():
    mysql = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="Lavani@01",
                                port="5432",
                                database="youtube_data")
    cursor = mysql.cursor()

    ch_list = []
    delete_query = '''drop table if exists playlists''' ## this will delete the table if already exist
    cursor.execute(delete_query)
    mysql.commit()

    try: 
        create_query = '''create table if not exists playlists(Playlist_Id varchar primary key,
                                                                    Title varchar,
                                                                    Channel_id varchar,
                                                                    Channel_name varchar,
                                                                    Published_at timestamp,
                                                                    Video_count int
                                                                    )'''
        cursor.execute(create_query)
        mysql.commit()
    except:
        print("playlist table created already")

    #   calling the mongo db and getting only channel info
    pl_list = []
    db = client["YoutubeData"]
    collection = db["channel_Details"]
    for pl_data in collection.find({},{"_id":0,"playlist_details":1}):
        for i in range ((len(pl_data["playlist_details"]))):
            pl_list.append((pl_data["playlist_details"][i]))
        df1 = pd.DataFrame(pl_list)
            
    for index,row in df1.iterrows():
            insert_query = '''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_id,
                                                Channel_name,
                                                Published_at,
                                                Video_count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)''' #all are column name  from table
                                                
            values = (row['Playlist_Id'],
                    row['Title'],
                    row['Channel_id'],
                    row['Channel_name'],
                    row['Published_at'],
                    row['Video_count'])
            
            
            cursor.execute(insert_query,values)
            mysql.commit()

#To insert Video details in SQL
def video_insert():
    mysql = psycopg2.connect(host="localhost",
                                    user="postgres",
                                    password="Lavani@01",
                                    port="5432",
                                    database="youtube_data")
    cursor = mysql.cursor()

    
    delete_query = '''drop table if exists videos''' ## this will delete the table if already exist
    cursor.execute(delete_query)
    mysql.commit()

    try: 
        create_query = '''create table if not exists videos(Channel_Name varchar(100),
                                                                Channel_id varchar(100),
                                                                Video_id varchar(30) primary key,
                                                                Title varchar(200),
                                                                Tags text,
                                                                Thumbnail varchar(200),
                                                                Description text ,
                                                                Published_Date timestamp,
                                                                Duration interval,
                                                                Views bigint,
                                                                Likes bigint,
                                                                Dislikes bigint,
                                                                Comments int,
                                                                Favourtite_Count int,
                                                                definition varchar(10),
                                                                Caption_status varchar(50)
                                                                    )'''
        cursor.execute(create_query)
        mysql.commit()
    except:
        print("videos table created already")
    vi_list = []
    db = client["YoutubeData"]
    collection = db["channel_Details"]
    for vi_data in collection.find({},{"_id":0,"Video_details":1}):
        for i in range ((len(vi_data["Video_details"]))):
            vi_list.append((vi_data["Video_details"][i]))
        df2 = pd.DataFrame(vi_list)
    for index,row in df2.iterrows():
                insert_query = '''insert into videos(Channel_Name,
                                                        Channel_id,
                                                        Video_id ,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favourtite_Count,
                                                        definition,
                                                        Caption_status)                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' #all are column name  from table
                                                    
                values = (row['Channel_Name'],
                        row['Channel_id'],
                        row['Video_id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favourtite_Count'],
                        row['definition'],
                        row['Caption_status'])
                cursor.execute(insert_query,values)
                mysql.commit()
                    

                    
# to insert Comments details in SQL
def comment_insert():
    mysql = psycopg2.connect(host="localhost",
                                        user="postgres",
                                        password="Lavani@01",
                                        port="5432",
                                        database="youtube_data")
    cursor = mysql.cursor()

    
    delete_query = '''drop table if exists comment''' ## this will delete the table if already exist
    cursor.execute(delete_query)
    mysql.commit()

    try:
        create_query = '''create table if not exists comment(Comment_id varchar primary key,
                                                                Video_id varchar,
                                                                Comment_text text,
                                                                Comment_author varchar,
                                                                Comment_publishdate timestamp
                                                                )'''

        cursor.execute(create_query)
        mysql.commit()
    except:
        print("comment table created already")
    com_list = []
    db = client["YoutubeData"]
    collection = db["channel_Details"]
    for com_data in collection.find({},{"_id":0,"comment_info":1}):
        for i in range ((len(com_data["comment_info"]))):
            com_list.append((com_data["comment_info"][i]))
        df3 = pd.DataFrame(com_list)

    for index,row in df3.iterrows():
                    insert_query = '''insert into comment(Comment_id,
                                                            Video_id,
                                                            Comment_text,
                                                            Comment_author,
                                                            Comment_publishdate
                                                            )
                                                        
                                                        values(%s,%s,%s,%s,%s)''' #all are column name  from table
                                                        
                    values = (row['Comment_id'],
                            row['Video_id'],
                            row['Comment_text'],
                            row['Comment_author'],
                            row['Comment_publishdate'])
                    cursor.execute(insert_query,values)
                    mysql.commit()
                    
#function to insert all the details in SQL
def table_insert():
    channel_insert()
    playlist_insert()
    video_insert()
    comment_insert()
    return "Table updated successfully"

def show_channel_table():
    ch_list = []
    db = client["YoutubeData"]
    collection = db["channel_Details"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    return df

def show_playlist_table():
    pl_list = []
    db = client["YoutubeData"]
    collection = db["channel_Details"]
    for pl_data in collection.find({},{"_id":0,"playlist_details":1}):
        for i in range ((len(pl_data["playlist_details"]))):
            pl_list.append((pl_data["playlist_details"][i]))
        df1 = st.dataframe(pl_list)
    return df1

def show_video_table():
    vi_list = []
    db = client["YoutubeData"]
    collection = db["channel_Details"]
    for vi_data in collection.find({},{"_id":0,"Video_details":1}):
        for i in range ((len(vi_data["Video_details"]))):
            vi_list.append((vi_data["Video_details"][i]))
        df2 = st.dataframe(vi_list)
    return df2

def show_comments_table():
    com_list = []
    db = client["YoutubeData"]
    collection = db["channel_Details"]
    for com_data in collection.find({},{"_id":0,"comment_info":1}):
        for i in range ((len(com_data["comment_info"]))):
            com_list.append((com_data["comment_info"][i]))
        df3 = st.dataframe(com_list)
    return df3

#To Create Streamlit inputs
st.set_page_config(
    page_title="My Streamlit App",
    page_icon=":shark:",
    layout="wide",
    initial_sidebar_state="expanded"
    
)   
with st.sidebar:
     st.title(":blue[Youtube Data harvesting and warehousing]")
     st.header("Takeaway Skills")
     st.caption("Python Scripting")
     st.caption("Data collection")
     st.caption("Mongo DB")     
     st.caption("API Integeration")
     st.caption("Data management using SQL")
channel_id = st.text_input("Enter your Channel id")


if st.button("Collect and store Data"):
     ch_ids =[]
     db = client["YoutubeData"]
     collection = db["channel_Details"]
     for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channal_id"])
     if channel_id in ch_ids:
          st.success("Given Channel details already exist")
     else:
          insert =channel_info(channel_id)
          st.success(insert)
if st.button("Insert to SQL"):
     Tables = table_insert()
     st.success(Tables)
show_table = st.radio("select the below table",("Channel info","Playlists","Videos","Comments"))
if show_table == "Channel info":
  show_channel_table()
elif  show_table == "Playlists":
  show_playlist_table()
elif show_table == "Videos":
     show_video_table()
elif show_table == "Comments":
     show_comments_table()  
     

# sql connection
mysql = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="Lavani@01",
                                port="5432",
                                database="youtube_data")
cursor = mysql.cursor()

Questions = st.selectbox("Select your questions",("1.What are the names of all the videos and their corresponding channels?",
                                                  "2.Which channels have the most number of videos, and how many videos do they have?",
                                                  "3.What are the top 10 most viewed videos and their respective channels?",
                                                  "4.How many comments were made on each video, and what are their corresponding video names?",
                                                  "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                  "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                  "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                  "8.What are the names of all the channels that have published videos in the year 2022?",
                                                  "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                  "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if Questions == "1.What are the names of all the videos and their corresponding channels?":
    query1 = '''select title as Videos,channel_name as Channelname from Videos'''
    cursor.execute(query1)
    mysql.commit()
    t1=cursor.fetchall()
    df = pd.DataFrame(t1,columns=["Video Name","Channel Name"])
    st.write(df)
    
elif Questions == "2.Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select channel_name as Channelname,total_vidoes as no_Vidoes from channel
                    order by total_vidoes  desc'''
    cursor.execute(query2)
    mysql.commit()
    t2=cursor.fetchall()
    df2 = pd.DataFrame(t2,columns=["Channel Name","No of Videos"])
    st.write(df2)
    
elif Questions == "3.What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select views as views,channel_name as Channelname,title as Video_name from videos
                    where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mysql.commit()
    t3=cursor.fetchall()
    df3 = pd.DataFrame(t3,columns=["Totol Views","Channel Name","No of Videos"])
    st.write(df3)
    
elif Questions == "4.How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select comments as no_comments ,title as Video_name from videos
                    where comments is not null'''
    cursor.execute(query4)
    mysql.commit()
    t4=cursor.fetchall()
    df4 = pd.DataFrame(t4,columns=["No Of Comments","Video Name"])
    st.write(df4)
    
elif Questions == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select title as Video_Title ,channel_name as Channelname,likes as Like_count from videos
                    where views is not null order by likes desc'''
    cursor.execute(query5)
    mysql.commit()
    t5=cursor.fetchall()
    df5 = pd.DataFrame(t5,columns=["Videotitle","Channel Name","Likes count"])
    st.write(df5)

elif Questions == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6 = '''select title as Video_Title ,likes as Like_count from videos'''
    cursor.execute(query6)
    mysql.commit()
    t6=cursor.fetchall()
    df6 = pd.DataFrame(t6,columns=["Videotitle","Likes count"])
    st.write(df6)
    
elif Questions == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select channel_name as channelname ,channel_views as Total_view from channel'''
    cursor.execute(query7)
    mysql.commit()
    t7=cursor.fetchall()
    df7 = pd.DataFrame(t7,columns=["Channel Name","Total Views"])
    st.write(df7)
    
elif Questions == "8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select channel_name as channelname ,title as Videoname,Published_date as Video_release from videos
                    where extract(year from published_date) = 2022'''
    cursor.execute(query8)
    mysql.commit()
    t8=cursor.fetchall()
    df8 = pd.DataFrame(t8,columns=["Channel Name","Video title","Published Year"])
    st.write(df8)
    
elif Questions == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select channel_name as channelname ,AVG(duration) as Average_Duration from videos group by channel_name'''
    cursor.execute(query9)
    mysql.commit()
    t9=cursor.fetchall()
    df9 = pd.DataFrame(t9,columns=["Channel Name","Average Duration"])
    T9 =[]
    for index,row in df9.iterrows():
        channel_title = row["Channel Name"]
        Avg_duration = row["Average Duration"]
        avg_duration_str = str(Avg_duration)
        T9.append(dict(channeltitle =channel_title,avgduration=avg_duration_str ))
    df1 = pd.DataFrame(T9)
    st.write(df1)
    
elif Questions == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select channel_name as channelname ,title as Video_title, comments as Comments from videos
                where comments is not null order by comments desc'''
    cursor.execute(query10)
    mysql.commit()
    t10=cursor.fetchall()
    df10 = pd.DataFrame(t10,columns=["Channel Name","Video Title","Total comments"])
    st.write(df10)
    









