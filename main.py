import supervisely_lib as sly
import pytube, os
import urllib.request, json
from supervisely_lib.io.fs import mkdir, clean_dir
from supervisely_lib.video_annotation.video_tag import VideoTag
from supervisely_lib.video_annotation.video_tag_collection import VideoTagCollection
from supervisely_lib.project.project_type import ProjectType
import pytube.exceptions as exceptions


my_app = sly.AppService()
api: sly.Api = my_app.public_api
TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
project_name = 'activity_net'
dataset_name = 'ds'
annotations_json = 'http://ec2-52-25-205-214.us-west-2.compute.amazonaws.com/files/activity_net.v1-3.min.json'
video_unavailable = 0
logger = sly.logger

work_dir = os.path.join(my_app.cache_dir, "work_dir")
mkdir(work_dir, True)


def download_from_youtube(url, folder):
    youtube = pytube.YouTube(url)
    video = youtube.streams.get_highest_resolution()
    video.download(folder)


def get_resolution(resolution_str):
    resolution = resolution_str.split('x')

    return (int(resolution[1]), int(resolution[0]))


def get_tags_with_frames(ann):
    result = {}
    for lbl in ann:
        tag_name = lbl['label']
        if tag_name not in result.keys():
            result[tag_name] = [lbl['segment']]
        else:
            result[tag_name].extend([lbl['segment']])

    return result


def get_frame_range(frames_ranges, file_info):
    result_frames_idxs = []
    for fr_range in frames_ranges:
        curr_frame_range = []
        first_find = False
        for idx, info in enumerate(file_info):
            if info > fr_range[0] and first_find is False:
                curr_frame_range.append(idx - 1)
                first_find = True
            if info >= fr_range[1]:
                curr_frame_range.append(idx)
                break
        if len(curr_frame_range) == 1:
            curr_frame_range.append(len(file_info) - 1)

        result_frames_idxs.append(curr_frame_range)

    return result_frames_idxs


new_project = api.project.create(WORKSPACE_ID, project_name, change_name_if_conflict=True, type=ProjectType.VIDEOS)
meta = sly.ProjectMeta()
ds = api.dataset.create(new_project.id, dataset_name, change_name_if_conflict=True)

with urllib.request.urlopen(annotations_json) as url:
    videos_data = json.loads(url.read().decode())
    annotations_data = videos_data['database']

for identifier in list(annotations_data.keys()):
    curr_video_data = annotations_data[identifier]
    print(identifier)
    curr_url = curr_video_data['url']
    curr_anns = curr_video_data['annotations']
    if len(curr_anns) == 0:
        continue
    print(curr_anns)
    resolution = get_resolution(curr_video_data['resolution'])

    try:
        download_from_youtube(curr_url, work_dir)
    except exceptions.VideoUnavailable:
        video_unavailable += 1
        continue

    video_name = os.listdir(work_dir)[0]
    video_path = os.path.join(work_dir, video_name)
    file_info = api.video.upload_paths(ds.id, [video_name], [video_path])

    curr_tags = []
    tags_with_frames = get_tags_with_frames(curr_anns)

    for tag_name, frames_ranges in tags_with_frames.items():
        if meta.get_tag_meta(tag_name) is None:
            tag_meta = sly.TagMeta(tag_name, sly.TagValueType.NONE)
            tag_collection = sly.TagMetaCollection([tag_meta])
            new_meta = sly.ProjectMeta(tag_metas=tag_collection)
            meta = meta.merge(new_meta)
            api.project.update_meta(new_project.id, meta.to_json())
        else:
            tag_meta = meta.get_tag_meta(tag_name)

        result_frames_idxs = get_frame_range(frames_ranges, file_info[0].frames_to_timecodes)

        if len(result_frames_idxs) == 1 and result_frames_idxs[0][0] == 0 and result_frames_idxs[0][1] == len(
                file_info[0].frames_to_timecodes) - 1:
            tag = VideoTag(tag_meta)
            curr_tags.append(tag)
        else:
            for frames_indexs in result_frames_idxs:
                tag = VideoTag(tag_meta, frame_range=frames_indexs)
                curr_tags.append(tag)

    tag_collection = VideoTagCollection(curr_tags)
    ann = sly.VideoAnnotation(resolution, file_info[0].frames_count, tags=tag_collection)
    logger.info('Create annotation for video {}'.format(video_name))
    api.video.annotation.append(file_info[0].id, ann)
    clean_dir(work_dir)

logger.info('{} videos was unavailable'.format(video_unavailable))