import json
import boto3
import os
import urllib.request
import urllib.parse
from datetime import datetime


# S3 client
s3 = boto3.client("s3")


# Environment variables
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]


def lambda_handler(event, context):

    try:

        # YouTube API endpoint
        url = "https://www.googleapis.com/youtube/v3/videos"


        # API parameters
        params = {

            "part": "snippet,statistics",

            "chart": "mostPopular",

            "regionCode": "US",

            "maxResults": 50,

            "key": YOUTUBE_API_KEY

        }


        # Convert parameters into URL format
        query_string = urllib.parse.urlencode(params)


        # Final API URL
        api_url = url + "?" + query_string


        # Call YouTube API
        with urllib.request.urlopen(api_url) as response:

            data = json.loads(
                response.read().decode("utf-8")
            )


        # Store extracted videos
        videos = []


        # Extract required fields
        for item in data["items"]:

            video = {

                "video_id": item["id"],

                "title": item["snippet"]["title"],

                "channel_id": item["snippet"]["channelId"],

                "channel_title": item["snippet"]["channelTitle"],

                "published_date": item["snippet"]["publishedAt"],

                "views": int(
                    item["statistics"].get(
                        "viewCount",
                        0
                    )
                ),

                "likes": int(
                    item["statistics"].get(
                        "likeCount",
                        0
                    )
                ),

                "comments": int(
                    item["statistics"].get(
                        "commentCount",
                        0
                    )
                ),

                "extraction_date": datetime.utcnow().strftime(
                    "%Y-%m-%d"
                )

            }


            videos.append(video)


        # Convert data to JSON
        json_data = json.dumps(
            videos,
            indent=4
        )


        # Create S3 path
        today = datetime.utcnow().strftime("%Y-%m-%d")


        s3_key = (
            f"bronze/youtube/"
            f"date={today}/"
            f"trending_videos.json"
        )


        # Upload JSON to S3
        s3.put_object(

            Bucket=BUCKET_NAME,

            Key=s3_key,

            Body=json_data,

            ContentType="application/json"

        )


        return {

            "statusCode": 200,

            "body": json.dumps({

                "message": "YouTube data stored successfully",

                "records": len(videos),

                "s3_path": s3_key

            })

        }


    except Exception as error:


        print(
            "ERROR:",
            str(error)
        )


        return {

            "statusCode": 500,

            "body": json.dumps({

                "message": "Pipeline failed",

                "error": str(error)

            })

        }
