import argparse
import datetime
import boto3
import requests
import json


class Ingestor:
    def __init__(self, url, per_page, bucket_name) -> None:
        self.url = url
        self.params = {
            "per_page": per_page,
            "strategy": 'new',
            "page": 1,
        }

        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")

    def get_response(self, **params):
        return requests.get(self.url, params=params)

    def get_data(self, **params):
        return self.get_response(**params).json()

    def save_data(self, data):

        new_data = bytes(json.dumps(data).encode("UTF-8"))
        sufix = self.url.strip("/").split("/")[-1]
        date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")

        (
            self.s3_client.put_object(
                Body=new_data,
                Bucket=self.bucket_name,
                Key=f"tabnews/{sufix}/{date_str}.json",
            )
        )

    def get_until_dates(self, date_start, date_stop):
        datetime_start = datetime.datetime.strptime(date_start, '%Y-%m-%d').date()
        datetime_stop = datetime.datetime.strptime(date_stop, '%Y-%m-%d').date()
        while datetime_start >= datetime_stop:
            
            print(self.params['page'])
            
            # coleta dado esalva no S3
            data = self.get_data(**self.params)
            self.save_data(data)
            
            self.params['page'] += 1
            datetime_start = datetime.datetime.fromisoformat(data[-1]['created_at']).date()


def main():

    datetime_now = datetime.datetime.now()
    date_now = datetime_now.strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser()
    parser.add_argument("--date_start", default=date_now)
    parser.add_argument("--delay", type=int, default=2)
    parser.add_argument("--url", type=str)
    parser.add_argument("--bucket", type=str)
    args = parser.parse_args()

    datetime_stop = datetime_now - datetime.timedelta(days=args.delay)
    date_stop = datetime_stop.strftime("%Y-%m-%d")


    ingestor = Ingestor(
        url=args.url,
        per_page=100,
        bucket_name=args.bucket,
    )

    ingestor.get_until_dates(date_now,date_stop)


if __name__ == "__main__":
    main()

