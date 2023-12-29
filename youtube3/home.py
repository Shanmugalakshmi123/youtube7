import streamlit as st
from googleapiclient.discovery import build
import subprocess
#import running
import os
from pprint import pprint
import sys
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from pandas.io import sql
import pymysql
import cryptography
import plotly.graph_objects as go
import pymongo
from pprint import pprint
#from state import provide_state

api_key="AIzaSyD8MTWFOkdY_kC1HrkUNOT5TuBHcil-jyM"
api_service="youtube"
api_version="v3"
youtube=build(api_service,api_version,developerKey=api_key)
def channel_details(ChannelId,youtube):
    
    request = youtube.channels().list(
            part="statistics,snippet,contentDetails,status",
            id=ChannelId
    )
    response=request.execute()
    #pprint(response)
    Channel_Name="hi"
    Channel_id="u"
    Subs_count=0
    Channel_views=0
    Channel_Description="us"
    Playlist_id="u"
    for i in response['items']: 
        Channel_Name=i["snippet"]["title"]
        Channel_id=i["id"]
        Subs_count=i['statistics']['subscriberCount']
        Channel_views=i['statistics']['viewCount']
        Video_Count=i['statistics']['videoCount']
        Channel_Description=i['snippet']['description']
        Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
        Channel_type=""
        Channel_status=i['status']['privacyStatus']
    Channel={"Channel_Name":Channel_Name,
             "Channel_Id":Channel_id,
             "Subscription":Subs_count,
             "Channel_Views":Channel_views,
             "Video_Count":Video_Count,
             "Channel_Description":Channel_Description,
             "Playlist_Id":Playlist_Id,
             "Channel_Status":Channel_status}
    return Channel
            
             
def playlist_details(Channel_Id,youtube):

    playlist_ids=[]
    playlist=[]
    token=None
    while True:
        request8=youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=Channel_Id,
            maxResults=10,
            pageToken=token
        
        )
        
        response8=request8.execute()
    # request = youtube.channels().list(
    #         part="statistics,snippet,contentDetails,status",
    #         id=Channel_Id
    #         )
    # response=request.execute()
    # Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    # while True:
    #     request=youtube.playlistItems().list(
    #             part="snippet,contentDetails",
    #             maxResults=50,
    #             playlistId=Playlist_Id,
    #             pageToken=token
    #         )
    #     response=request.execute()
        #pprint(response)
        # for i in response8['items']:
        #     playlist_ids.append({"playlist_id":Playlist_Id})
        # token=response8.get("nextPageToken")
        # print(token)
        # if token is None:
        #     break
        pprint(response8)
        for i in (response8['items']):
            Playlist_Id=i["id"]
            Playlist_name=i["snippet"]["title"]
            Channel_id=i["snippet"]["channelId"]
            playlist.append({"Playlist_id":Playlist_Id,
                    "Playlist_name":Playlist_name,
                    "Channel_id":Channel_id})
        token=response8.get('nextPageToken')
        if token is None:
            break
       # print(token)
       
    return playlist

def get_video_ids(Channel_Id,youtube):
    
    request = youtube.channels().list(
            part="statistics,snippet,contentDetails,status",
            id=Channel_Id
            )
    response=request.execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    print(Playlist_Id)
    token=None
    video=[]
    video_ids=[]
    while True:
        request=youtube.playlistItems().list(
                part="snippet,contentDetails",
                maxResults=50,
                playlistId=Playlist_Id,
                pageToken=token
            )
        response=request.execute()
        #pprint(response)
        for i in response['items']:
            video_ids.append({"video_id":i['contentDetails']['videoId'],"playlist_id":Playlist_Id})
        token=response.get("nextPageToken")
        #print(token)
        if token is None:
            break
    
    return video_ids   
    
def video_details(Channel_Id,youtube):
    
    video_ids=get_video_ids(Channel_Id,youtube)
    video=[]
    for j in range(len(video_ids)): #res1['items']:
            #video_ids.append(res1['items'][j]['contentDetails']['videoId'])
            request2=youtube.videos().list(
                part="statistics,snippet,contentDetails",
                id=video_ids[j].get("video_id"),
                maxResults=50
            )
            
            response2=request2.execute()
            
            for i in range(len(response2['items'])):
                Video_Id=(response2['items'][i]["id"])
                Video_Name=(response2['items'][i]["snippet"]["title"])
                Video_Description=(response2['items'][i]["snippet"]["description"])
                Tags=(response2['items'][i]["etag"])
                PublishedAt=(response2['items'][i]["snippet"]["publishedAt"])
                View_Count=(response2['items'][i]["statistics"]["viewCount"])
                Like_Count=(response2['items'][i]["statistics"]["likeCount"])
                Dislike_Count=(0)
            # Dislike_Count=i["statistics"]["dislikeCount"]
                Favorite_Count=(response2['items'][i]["statistics"]["favoriteCount"])
                Comment_Count=(response2['items'][i]["statistics"]["commentCount"])
                Duration=(response2['items'][i]["contentDetails"]["duration"])
                #Thumbnail=i["processingDetails"]["thumbnailsAvailability"]
                #Thumbnail.append("")
                Caption_Status=(response2['items'][i]["contentDetails"]["caption"])
                channel_id1=(response2['items'][i]["snippet"]["channelId"])
            video.append({"Video_Id":Video_Id,
                "Video_Name":Video_Name,
                "Playlist_Id":video_ids[j].get("playlist_id"),
                "Video_Description":Video_Description,
                "Tags":Tags,
                "PublishedAt":PublishedAt,
                "ViewCount":View_Count,
                "LikeCount":Like_Count,
                "DislikeCount":Dislike_Count,
                "FavoriteCount":Favorite_Count,
                "CommentCount":Comment_Count,
                "Duration":Duration,
                #"Thumbnail":Thumbnail[i],
                "CaptionStatus":Caption_Status,
                "ChannelId":channel_id1})
    print("completed")
    return video

def comment_details(Channel_Id,youtube):

    video_ids=get_video_ids(Channel_Id,youtube)
    
    commentids=[]
    comments=[]
    for l in range(len(video_ids)):
        request5=youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_ids[l].get("video_id"),
            maxResults=20
        )
        commentids=[]
        #Comment_Id3=""
        response5=request5.execute()
        for k in range(len(response5['items'])): #res1['items']:
            commentids.append(response5['items'][k]['id'])
        #print(i,commentids[i])
        for j in range(len(commentids)):
            request11=youtube.comments().list(
            part="snippet",
            id=commentids[j]
            #maxResults=20
            )
            response11=request11.execute()
        # pprint(response)
            for i in response11['items']:
                Comment_Text1=i["snippet"]["textDisplay"]
                Video_Id=video_ids[l].get("video_id")
                Comment_Author1=i["snippet"]["authorDisplayName"]
                Comment_PublishedAt1=i["snippet"]["publishedAt"]
                comments.append({ "CommentId":commentids[j],
                        "CommentText":Comment_Text1,
                        "VideoId":Video_Id,
                        "CommentAuthor":Comment_Author1,
                        "Comment_PublishedAt":Comment_PublishedAt1})
    print("completed2")
    return comments


def main(Channel_Id):
    c=channel_details(Channel_Id,youtube)
    p=playlist_details(c['Channel_Id'],youtube)
    v=video_details(c['Channel_Id'],youtube)
    cm=comment_details(c['Channel_Id'],youtube)
    data={'channel_details':c,
          'playlist_details':p,
          'video_details':v,
          'comment_details':cm}
    return data

def to_mongo(data):
    client=pymongo.MongoClient("mongodb://localhost:27017/")
    db=client["youtube3"]
    coll=db['youtube']
    coll.insert_one(data)

def tomysql(Channel_Name):
    client=pymongo.MongoClient("mongodb://localhost:27017/")
    db=client["youtube3"]
    coll=db['youtube']
    channel1=coll.find_one({"channel_details.Channel_Name":Channel_Name})
    channel=channel1.get("channel_details")
    mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="Shanthini12345*",
    database="youtube15"
    )
    mycursor=mydb.cursor()
    data1=(channel.get('Channel_Id'),channel.get('Channel_Name'),channel.get('Channel_type'),channel.get('Channel_Views'),channel.get('Channel_Description'),channel.get('Channel_status'),channel.get('Playlist_Id'))
    #pprint(data1)
    mycursor.execute("insert into channel1 values (%s,%s,%s,%s,%s,%s,%s)",data1)



    #playlist
    playlist1=coll.find_one({"channel_details.Channel_Name":Channel_Name})
    playlist=playlist1.get("playlist_details")
    for i in range(len(playlist)):
        data2=(playlist[i].get('Playlist_id'),playlist[i].get('Channel_id'),playlist[i].get('Playlist_name'))
        #pprint(data2)
        mycursor.execute("insert into playlist values (%s,%s,%s)",data2)



    #videos
    video1=coll.find_one({"channel_details.Channel_Name":Channel_Name})
    video=video1.get("video_details")
    for i in range(len(video)):
        PublishedAt=video[i].get('PublishedAt')
        Playlist_Id=video[i].get('Playlist_Id')
        date1=pd.Timestamp(PublishedAt)
        Duration=video[i].get('Duration')
        t=Duration.replace("PT","")
        #print(t)
        # x=[]
        # x2=[]
        # for j in t:
        #     if j=="H":
        #         t=t.replace("H","")
        #         break
        #     else:
        #         x2.append(j)

        # for j in t:

        #     if j=="M":
        #         t=t.replace("M","")
            
        #         break
        #     else:
        #         x.append(j)
        #         print("x=",x)
        #         t=t.replace(j,"")
        #         print(t)
        # t=t.replace("S","")
        # print("t=",t)
        # x1="".join(x)
        # x1=x1.replace("S","")
        # x1.replace("H","")
        # x1.replace("H","")
        # print("x1=",x1)
        # x1.replace("H","")
        # if t=="":
        #     time1=int(x1)*60
        # else:
        #     time1=int(x1)*60+int(t)
        #duration="PT11S"
        #t=duration
        #t.replace("PT","")
        h=[]
        m=[]
        s=[]
        x=[]
        h1=""
        m1=""
        s1=""
        for j in t:
            if not j.isalpha():
                x.append(str(j))
            else:
                if j=='H':
                    h=x
                    
                    x=[]
                elif j=='M':
                    m=x
                    
                    x=[]
                elif j=='S':
                    s=x
                    
                    x=[]
        h1="".join(h)
        m1="".join(m)
        s1="".join(s)
        if h1=="":
            h1="0"
        if m1=="":
            m1="0"
        if s1=="":
            s1="0"
        print(h1,m1,s1)
        h1=int(h1)
        m1=int(m1)
        s1=int(s1)
        time1=h1*3600+m1*60+s1
        print(time1)

        data3=(video[i].get('Video_Id'),video[i].get('Playlist_Id'),video[i].get('Video_Name'),video[i].get('Video_Description'),date1,video[i].get('ViewCount'),video[i].get('LikeCount'),video[i].get('DislikeCount'),video[i].get('FavoriteCount'),video[i].get('CommentCount'),time1,str(video[i].get('Thumbnail')),video[i].get('CaptionStatus'))
        #st.write(data3)
        mycursor.execute("insert into video values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",data3)
        
        


    #comments
    comment1=coll.find_one({"channel_details.Channel_Name":Channel_Name})
    comments=comment1.get("comment_details")
    for i in range(len(comments)):
        date1=pd.Timestamp(comments[i].get('Comment_PublishedAt1'))
        
            #comments[i].update(Comment_Author1="Anonymous")
        data5=(comments[i].get('CommentId'),comments[i].get('VideoId'),comments[i].get('CommentText'),comments[i].get('CommentAuthor'),date1)
        #st.write(data5)
        mycursor.execute("insert into comment1 values (%s,%s,%s,%s,%s)",data5)
        mydb.commit()

def view():
    my_conn = create_engine("mysql+pymysql://root:Shanthini12345*@localhost/youtube15")
    myresult=pd.read_sql("select * from channel1",my_conn)
    st.write("Channel details",myresult)
    myresult=pd.read_sql("select * from playlist",my_conn)
    st.write("Playlist details",myresult)
    myresult=pd.read_sql("select * from video",my_conn)
    st.write("Video details",myresult)
    myresult=pd.read_sql("select * from comment1",my_conn)
    st.write("Comment details",myresult)

def name_list():
    client=pymongo.MongoClient("mongodb://localhost:27017/")
    db=client["youtube3"]
    coll=db['youtube']
    myresult=coll.distinct("channel_details.Channel_Name")
    return myresult
#Channel_Name="Schafer5"
#channel_details(Channel_Name,youtube)
#state.inputs=state.inputs or set()
x=""
#options=[]
Channel_Id=st.sidebar.text_input("Enter the Channel Id",placeholder="Enter the channel Id")
choice=st.sidebar.selectbox("Setect the option",("Scrap the channel","Migrate the channel","View channel details","Answer Questions"),index=None,placeholder="Select")

#ch=st.sidebar.selectbox("Select a channel",options=list(state.inputs),index=None,placeholder="Select")
if choice=="Scrap the channel":
    data=main(Channel_Id)
    to_mongo(data)
    x=(data.get("channel_details").get("Channel_Name"))
    #state.inputs.add(x)
    #options=(x,)
    st.write("Channel details",data)
elif choice=="Migrate the channel":
    ch=st.sidebar.selectbox("Select a channel",options=list(name_list()),index=None,placeholder="Select")   
    if ch is not None:
        tomysql(ch)
elif choice=="View channel details":
    view()
elif choice=="Answer Questions":
    my_conn = create_engine("mysql+pymysql://root:Shanthini12345*@localhost/youtube15")
    qns=st.sidebar.selectbox("Select a question",("What are the names of all the videos and their corresponding channels?","Which channels have the most number of videos, and how many videos do they have?","What are the top 10 most viewed videos and their respective channels?","How many comments were made on each video, and what are their corresponding video names?","Which videos have the highest number of likes, and what are their corresponding channel names?","What is the total number of likes and dislikes for each video, and what are their corresponding video names?","What is the total number of views for each channel, and what are their corresponding channel names?","What are the names of all the channels that have published videos in the year 2022?","What is the average duration of all videos in each channel, and what are their corresponding channel names?"),index=None,placeholder="Select")

    if qns=="What are the names of all the videos and their corresponding channels?":
        result=pd.read_sql("select channel_name,video_name from channel1,video where channel1.Playlist_Id=video.Playlist_id",my_conn)
        st.write(result)
    elif qns=="Which channels have the most number of videos, and how many videos do they have?":
        result=pd.read_sql("select Channel_Name,count(Video_Id) from channel1 join video on channel1.Playlist_id=video.Playlist_id group by Channel_Id order by count(Video_Id) desc;",my_conn)
        st.write(result)
    elif qns=="What are the top 10 most viewed videos and their respective channels?":
        result=pd.read_sql("select channel_name,video_name,view_count from channel1,video where channel1.Playlist_Id=video.Playlist_id order by view_count desc limit 10;",my_conn)
        st.write(result)
    elif qns=="How many comments were made on each video, and what are their corresponding video names?":
        result=pd.read_sql("select video_name,comment_count from video order by comment_count desc",my_conn)
        st.write(result)
    elif qns=="Which videos have the highest number of likes, and what are their corresponding channel names?":
        result=pd.read_sql("select channel_name,video_name,like_count from channel1,video where channel1.Playlist_Id=video.Playlist_id order by like_count desc",my_conn)
        st.write(result)
    elif qns=="What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        result=pd.read_sql("select video_name,like_count from video  order by like_count desc",my_conn)
        st.write(result)
    elif qns=="What is the total number of views for each channel, and what are their corresponding channel names?":
        result=pd.read_sql("select channel_name,channel_views from channel1  order by channel_views desc",my_conn)
        st.write(result)
    elif qns=="What are the names of all the channels that have published videos in the year 2022?":
        result=pd.read_sql("select channel_name,Published_Date from channel1,video  where channel1.Playlist_Id=video.Playlist_id and year(Published_Date)=2022",my_conn)
        st.write(result)
    elif qns=="What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        result=pd.read_sql("select channel_name,avg(duration) from channel1,video where channel1.Playlist_Id=video.Playlist_id group by channel_name",my_conn)
        st.write(result)
    elif qns=="What is the total number of comments for each video, and what are their corresponding video names?":
        result=pd.read_sql("select video_name,comment_count from video  order by comment_count desc",my_conn)
        st.write(result)
   
    
#select channel_id,video_id,view_count from channel1,video where channel1.channel_id=(select channel_id from playlist where playlist_id =(select playlist_id from video order by view_count desc limit 10)
