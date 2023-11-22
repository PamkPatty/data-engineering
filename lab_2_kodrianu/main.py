import zipfile
import aiohttp
import asyncio
import os

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


async def download_file(session, url):
    print("Processing: " + url)
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Error with URL: {url} , skip")
            return

        file_name = os.path.join('downloads', os.path.basename(url))
        with open(file_name, 'wb') as file:
            while True:
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                file.write(chunk)
        print(f"Завантажено: {file_name}")

        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall('downloads')
        os.remove(file_name)
        print("zip deleted")
    

async def main():
    if not os.path.exists('downloads'):
        os.makedirs('downloads')
    
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url) for url in download_uris]
        await asyncio.gather(*tasks)
    
    if os.path.isdir("downloads"):
        print(os.listdir("downloads"))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())