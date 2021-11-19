import pytube
import urllib.request, json
from multiprocessing import Pool
import pytube.exceptions as exceptions
import supervisely_lib as sly
from supervisely_lib.io.fs import mkdir


work_dir = '/home/andrew/alex_work/app_cache/videos_tube'
mkdir(work_dir, True)
annotations_json = 'http://ec2-52-25-205-214.us-west-2.compute.amazonaws.com/files/activity_net.v1-3.min.json'
logger = sly.logger
video_unavailable = 0

def download_from_youtube(url):
    global video_unavailable
    youtube = pytube.YouTube(url)
    try:
        video = youtube.streams.get_highest_resolution()
        video.download(work_dir, filename_prefix=youtube.video_id)
    except exceptions.VideoUnavailable:
        logger.info('{} video is unavailable'.format(url))
        video_unavailable += 1


if __name__ == '__main__':

    urls = []
    with urllib.request.urlopen(annotations_json) as url:
        videos_data = json.loads(url.read().decode())
        annotations_data = videos_data['database']

        for identifier in list(annotations_data.keys()):
            curr_video_data = annotations_data[identifier]
            curr_anns = curr_video_data['annotations']
            if len(curr_anns) == 0:
                continue
            curr_url = curr_video_data['url']
            urls.append(curr_url)

    with Pool() as p:
        p.map(download_from_youtube, urls)

    logger.info('{} videos was unavailable'.format(video_unavailable))
