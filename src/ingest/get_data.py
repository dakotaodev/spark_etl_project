from TikTokApi import TikTokApi
import asyncio
import os
import json

ms_token = "YLzdW9NR9Y8D3BK5X-aMOWljh8_LBjfgw3LXhfM7OKeiT0ZvqEFO72bxc9tQMN8Y0eh1thdLuQoUWXchqysvac-ZeglpS_hzY0jTyw-VwMVIia8hbPmgitb4vtwBt9vzxpIWnEkmyOayfqi9qRM3L7Fy"


# ms_token = os.environ.get(
#     "ms_token", None
# )  # set your own ms_token, think it might need to have visited a profile


async def user_example():
    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[ms_token], 
                                  num_sessions=1, 
                                  sleep_after=3, 
                                  browser=os.getenv("TIKTOK_BROWSER", "chromium"),
                                  headless=False
                                )
        user = api.user("brookeoberhauser")
        user_data = await user.info()
        print(user_data)

        async for video in user.videos(count=1):
            data = video.as_dict
            # Open the file in write mode ('w') and use json.dump()
            filename=f"video_data_{data["createTime"]}.json"
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
                print(f"Data successfully written to {filename}")
            except IOError as e:
                print(f"Error writing to file: {e}")
                # save the video to JSON
            

if __name__ == "__main__":
    asyncio.run(user_example())