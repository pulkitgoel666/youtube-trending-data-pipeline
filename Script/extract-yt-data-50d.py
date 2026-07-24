import json
import boto3
import os
import urllib.request
import urllib.parse
import urllib.error
import time
from datetime import datetime, timedelta


# --------------------------------
# AWS S3 Client
# --------------------------------

s3 = boto3.client("s3")


# --------------------------------
# Environment Variables
# --------------------------------

YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]


# --------------------------------
# YouTube API Request Function
# --------------------------------

def call_youtube_api(url, params):

    query_string = urllib.parse.urlencode(params)

    full_url = url + "?" + query_string


    for attempt in range(3):

        try:

            with urllib.request.urlopen(full_url) as response:

                return json.loads(
                    response.read().decode("utf-8")
                )


        except urllib.error.HTTPError as error:

            if error.code == 429:

                wait_time = (attempt + 1) * 5

                print(
                    f"Rate limited. Waiting {wait_time} seconds"
                )

                time.sleep(wait_time)

            else:

                raise error


    raise Exception(
        "YouTube API failed after retries"
    )


# --------------------------------
# Lambda Handler
# --------------------------------

def lambda_handler(event, context):

    try:

        # --------------------------------
        # Last 50 days filter
        # --------------------------------

        start_date = (
            datetime.utcnow() - timedelta(days=50)
        ).strftime("%Y-%m-%dT00:00:00Z")


        # --------------------------------
        # Search keywords
        # --------------------------------

        keywords = [
            "technology",
            "AI"
        ]


        video_ids = set()


        search_url = (
            "https://www.googleapis.com/youtube/v3/search"
        )


        # --------------------------------
        # Extract Video IDs
        # --------------------------------

        for keyword in keywords:


            next_page_token = None

            page_count = 0


            while True:


                page_count += 1


                # Limit pages to avoid quota exhaustion

                if page_count > 5:

                    break


                search_params = {

                    "part": "snippet",

                    "q": keyword,

                    "type": "video",

                    "publishedAfter": start_date,

                    "order": "viewCount",

                    "maxResults": 30,

                    "key": YOUTUBE_API_KEY

                }


                if next_page_token:

                    search_params["pageToken"] = next_page_token



                search_response = call_youtube_api(

                    search_url,

                    search_params

                )


                for item in search_response.get(
                    "items",
                    []
                ):

                    video_ids.add(
                        item["id"]["videoId"]
                    )


                next_page_token = search_response.get(
                    "nextPageToken"
                )


                if not next_page_token:

                    break



                time.sleep(1)



        print(
            f"Total unique videos found: {len(video_ids)}"
        )


        # --------------------------------
        # Get Video Details
        # --------------------------------

        videos_url = (
            "https://www.googleapis.com/youtube/v3/videos"
        )


        final_data = []


        video_ids = list(video_ids)


        # YouTube accepts max 50 IDs

        for i in range(0, len(video_ids), 50):


            batch_ids = video_ids[i:i+50]


            video_params = {

                "part": "snippet,statistics",

                "id": ",".join(batch_ids),

                "key": YOUTUBE_API_KEY

            }


            video_response = call_youtube_api(

                videos_url,

                video_params

            )


            for video in video_response.get(
                "items",
                []
            ):


                statistics = video.get(
                    "statistics",
                    {}
                )


                snippet = video.get(
                    "snippet",
                    {}
                )


                record = {


                    "video_id":
                    video["id"],


                    "title":
                    snippet.get(
                        "title"
                    ),


                    "channel_id":
                    snippet.get(
                        "channelId"
                    ),


                    "channel_title":
                    snippet.get(
                        "channelTitle"
                    ),


                    "published_date":
                    snippet.get(
                        "publishedAt"
                    ),


                    "views":
                    int(
                        statistics.get(
                            "viewCount",
                            0
                        )
                    ),


                    "likes":
                    int(
                        statistics.get(
                            "likeCount",
                            0
                        )
                    ),


                    "comments":
                    int(
                        statistics.get(
                            "commentCount",
                            0
                        )
                    ),


                    "extraction_date":
                    datetime.utcnow().strftime(
                        "%Y-%m-%d"
                    )

                }


                final_data.append(record)



        # --------------------------------
        # S3 Partition Path
        # --------------------------------

        now = datetime.utcnow()


        year = now.strftime("%Y")

        month = now.strftime("%m")

        day = now.strftime("%d")


        s3_key = (

            f"bronze/"
            f"year={year}/"
            f"month={month}/"
            f"day={day}/"
            f"youtube_historical_videos.json"

        )


        # --------------------------------
        # Upload JSON to S3
        # --------------------------------

        s3.put_object(

            Bucket=BUCKET_NAME,

            Key=s3_key,

            Body=json.dumps(
                final_data,
                indent=4,
                ensure_ascii=False
            ),

            ContentType="application/json"

        )


        return {


            "statusCode": 200,


            "body": json.dumps({

                "message":
                "YouTube ingestion completed",


                "videos":
                len(final_data),


                "s3_path":
                s3_key

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

                "message":
                "Pipeline failed",


                "error":
                str(error)

            })

        }
