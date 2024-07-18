import time
import requests
import datetime as dt
import pandas as pd
import os


class RedditException(Exception): ...


class Token:
    expired = True
    expire_time = None

    def __init__(self, expire_time: dt.datetime) -> None:
        self.expired = False
        self.expire_time = expire_time

        # give expiration
        now = dt.datetime.now()
        token_time_left = self.expire_time - now

        print(
            f'Token valid for {int(token_time_left.seconds / 60)} min and expires at '
            f'{self.expire_time.strftime("%H:%M")}'
        )

    def check_expiration(self, show=True):
        if self.expire_time is None:
            return False

        now = dt.datetime.now()

        if self.expire_time < now:
            self.expired = True
            if show:
                print("Token Expired")
            return False
        else:
            token_time_left = self.expire_time - now
            if show:
                print(f"Token valid for {int(token_time_left.seconds / 60)} min")
            return True


class Reddit:
    # headers
    headers = {"User-Agent": "Python script:Trends v0.2 by u/molivo10"}

    token: Token

    # urls
    url_base = r"https://oauth.reddit.com"

    # params
    limit = 100
    # util_lists
    top_list = ["all", "year", "month", "week", "day", "hour"]

    cols = [
        "timestamp",
        "title",
        "post_hint",
        "link_flair_text",
        "selftext",
        "ups",
        "downs",
        "upvote_ratio",
        "num_comments",
        "num_crossposts",
        "total_awards_received",
        "score",
        "gilded",
        "archived",
        "subreddit_name_prefixed",
        "subreddit_subscribers",
        "domain",
        "url_overridden_by_dest",
        "preview",
        "url",
        "permalink",
        "id",
        "name",
        "author_fullname",
        "created_utc",
    ]

    def __init__(self):
        self.authorize()

    def authorize(self):
        CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
        CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
        REDDIT_USERNAME = os.environ.get("REDDIT_USERNAME")
        REDDIT_PASSWORD = os.environ.get("REDDIT_PASSWORD")

        if not all([CLIENT_ID, CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD]):
            raise RedditException(
                "Could not read environment variables:"
                "REDDIT_CLIENT "
                "REDDIT_CLIENT_SECRET "
                "REDDIT_USERNAME "
                "REDDIT_PASSWORD "
            )

        client_auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)

        data = {
            "grant_type": "password",
            "username": REDDIT_USERNAME,
            "password": REDDIT_PASSWORD,
        }

        r = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=client_auth,
            data=data,
            headers=self.headers,
        )

        print(r.request.headers)

        if not r.ok:
            raise Exception("Could not get token")
        else:
            token: str | None = r.json().get("access_token") if r.json() else None
            if token is None:
                raise RedditException(
                    "Could not obtain access token, check that credentials are OK"
                )

            # update
            self.headers["Authorization"] = f"bearer {token}"

            expire_time = dt.datetime.now() + dt.timedelta(hours=1)
            self.token = Token(expire_time)

            print("Got token Successfully")

    @staticmethod
    def transform_df(r_json):
        df = pd.DataFrame([item["data"] for item in r_json["data"]["children"]])

        df["timestamp"] = df["created_utc"].apply(
            lambda x: dt.datetime.fromtimestamp(x)
        )
        return df

    def util_get(self, url, params):
        get_url = self.url_base + url
        count = 0
        after = True
        result = []

        while after:
            params["after"] = after

            r = requests.get(get_url, headers=self.headers, params=params)

            result.append(self.transform_df(r.json()))

            # harvest
            after = r.json()["data"]["after"]

            count += self.limit
            print(f"count:{count}", end="\t" * 2)
            print(
                f'x-rate remaining {r.headers["x-ratelimit-remaining"]}  x-rate reset: '
                f'{r.headers["x-ratelimit-reset"]}'
            )

        print("Max results, breaking")
        return pd.concat(result)

    def aggregate(self, subreddit):
        df_list = [
            self.subreddit(subreddit, sort=sort) for sort in ["rising", "new", "hot"]
        ] + [
            self.subreddit_top(subreddit, sort=sort, upto="month")
            for sort in ["top", "controversial"]
        ]

        return pd.concat(df_list)

    def popular_subreddits(self):
        url = "/subreddits/popular"

        result = []
        params = dict(limit=self.limit, show="all", sr_detail="true")

        df = self.util_get(url, params)

        df["subs_rank"] = df["subscribers"].rank(ascending=False)
        df[["name", "url", "title", "subscribers", "subs_rank"]].to_csv(
            f'popular//{dt.datetime.now().strftime("%Y-%m-%d")}_popular.csv',
            index=False,
        )

    def search(self, query, subreddit=None, sort="comments", top="month"):
        """
        todo: /best
        subreddit: None or subreddit
        sort ['relevance', ' hot', ' top', ' new', ' comments']
        top ['hour', ' day', ' week', ' month', ' year', ' all']
        """
        params = dict(
            q=query, limit=self.limit, restric_sr=False, show="all", sort=sort, t=top
        )

        if subreddit:
            url = rf"/r/{subreddit}/search"
            params.update({"restrict_sr": True, "type": "link "})  # type: ['sr','link','user']

            df = self.util_get(url, params)
            return df
        else:
            url = r"/search"
            df = self.util_get(url, params)

            return df

    def subreddit(self, subreddit, sort="new"):
        """
        sort = ['rising', 'random', 'new', 'hot']
        """

        url = f"/r/{subreddit}/{sort}"

        params = dict(limit=self.limit, show="all", after=None)
        print(f"Getting {url}")
        df = self.util_get(url, params)

        return df

    def subreddit_top(self, subreddit, sort="top", upto="month"):
        """
        sort : ['top', 'controversial']
        upto list : ['all', 'year', 'month', 'week', 'day', 'hour']
        """

        url = f"/r/{subreddit}/{sort}"

        result = []
        slicer = self.top_list.index(
            upto
        )  # slices from e.g. 'month' to end e.g.'hour' by getting index and using it to slice

        for top in self.top_list[slicer:]:
            print(f"Getting {url} \t for {top}")
            params = dict(t=top, limit=self.limit, show="all")

            df = self.util_get(url, params)

            result.append(df)

            if len(df) < 900:
                print(f"{top} contains all recent results")
                break

        df = pd.concat(result)
        df.reset_index(drop=True, inplace=True)

        return df

    def comments(self, subreddit, link):
        """
        Post(Link) Comments
        """
        rf"/r/{subreddit}/comments/{link}"

        params = dict(
            article=link,
            # comment =  #(optional) ID36 of a comment
            context=8,  # an integer between 0 and 8
            # depth =    #(optional) an integer
            # limit      #(optional) an integer
            showedits=False,  # boolean value
            showmedia=True,  # boolean value
            showmore=True,  # boolean value
            showtitle=True,  # boolean value
            sort="confidence",  # one of (confidence, top, new, controversial, old, random, qa, live)
            sr_detail=False,  # (optional) expand subreddits
            threaded=False,  # boolean value
            truncate=0,
        )  # an integer between 0 and 50

    def user_profile(self, username, get_pics=False, get_vids=False):
        """
        todo: /user/username/comments
              /user/username/upvoted
              /user/username/downvoted
              /user/username/hidden
              /user/username/saved
              /user/username/gilded

        sort [hot, new, top, controversial]
        t [hour, day, week, month, year, all]
        type ['links','comments']
        """
        url = f"/user/{username}/submitted"

        params = dict(
            limit=self.limit,
            context=2,
            show="given",
            sort="new",
            t="all",
            type="all",
            raw_json=1,
            after=None,
        )

        df = self.util_get(url, params)

        df["created_utc"] = df["created_utc"].apply(dt.datetime.fromtimestamp)

        def write_file(row, content, url: str):
            ext = url.rsplit(".", 1)[-1]
            date = str(row["created_utc"])
            date = date.replace(":", "-")

            os.makedirs(rf'Profiles/{row["author"]}/{row["subreddit"]}', exist_ok=True)

            with open(
                rf'Profiles/{row["author"]}/{row["subreddit"]}/{date}.{ext}', "wb"
            ) as f:
                f.write(content)

            print(
                "saved",
                rf'Profiles/{row["author"]}/{row["subreddit"]}/{date}.{ext}',
                end="\n" * 2,
            )

        if get_pics:
            # for row,x in df[['preview','subreddit','created_utc','author']].dropna(subset=['preview']).iterrows():
            for idx, row in df.drop_duplicates(subset=["id"]).iterrows():
                if row["domain"] == "i.redd.it" or any(
                    [ext in row["url"] for ext in [".jpg", ".png", ".jpeg"]]
                ):
                    url = row["url"]
                elif row["post_hint"] == "link":
                    url = row["preview"]["images"][0]["source"]["url"]
                else:
                    continue

                print(url, end="\t")

                r = requests.get(url)
                write_file(row, r.content, url)

        if get_vids:
            for row, row in (
                df[df["domain"] == "redgifs.com"]
                .drop_duplicates(subset=["id"])
                .dropna(subset=["preview"])
                .iterrows()
            ):
                url = row["preview"]["reddit_video_preview"]["fallback_url"]
                print(url, end="\t")
                r = requests.get(url)
                write_file(row, r.content, url)

        return df


timer = time.perf_counter_ns()
r = Reddit()
df = r.user_profile("Avereniect")
timer = time.perf_counter_ns() - timer
print(f"took {timer/1_000_000}ms")
