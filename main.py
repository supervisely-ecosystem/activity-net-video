import supervisely_lib as sly
import pytube, os
import urllib.request, json
from supervisely_lib.io.fs import mkdir
from supervisely_lib.video_annotation.video_tag import VideoTag
from supervisely_lib.project.project_type import ProjectType


my_app = sly.AppService()
api: sly.Api = my_app.public_api
TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
project_name = 'activity_net'
dataset_name = 'ds'
annotations_json = 'http://ec2-52-25-205-214.us-west-2.compute.amazonaws.com/files/activity_net.v1-3.min.json'

work_dir = os.path.join(my_app.cache_dir, "work_dir")
mkdir(work_dir, True)


def download_from_youtube(url, folder):
    youtube = pytube.YouTube(url)
    video = youtube.streams.first()
    video.download(folder)


new_project = api.project.create(WORKSPACE_ID, project_name, change_name_if_conflict=True, type=ProjectType.VIDEOS)
meta = sly.ProjectMeta()
ds = api.dataset.create(new_project.id, dataset_name, change_name_if_conflict=True)

with urllib.request.urlopen(annotations_json) as url:
    videos_data = json.loads(url.read().decode())
    annotations_data = videos_data['database']

for batch in sly.batched(list(annotations_data.keys()), batch_size=10):
    for identifier in batch:
        curr_video_data = annotations_data[identifier]
        curr_url = curr_video_data['url']
        curr_anns = curr_video_data['annotations']

        curr_tags = []
        # for curr_ann in curr_anns:
        #     tag_name = curr_ann['label']
        #     tag_range = curr_ann['segment']
        #     if meta.get_tag_meta(tag_name) is None:
        #         tag_meta = sly.TagMeta(tag_name, sly.TagValueType.NONE)
        #         tag_collection = sly.TagMetaCollection([tag_meta])
        #         new_meta = sly.ProjectMeta(tag_metas=tag_collection)
        #         meta = meta.merge(new_meta)
        #         api.project.update_meta(new_project.id, meta.to_json())
        #     else:
        #         tag_meta = meta.get_tag_meta(tag_name)
        #
        #     tag = VideoTag(tag_meta)
        #     curr_tags.append(tag)


        download_from_youtube(curr_url, work_dir)

        video_name = os.listdir(work_dir)[0]
        video_path = os.path.join(work_dir, video_name)
        # old_video_name = os.listdir(work_dir)[0]
        # new_video_name = old_video_name.replace(' ', '_')
        # old_video_path = os.path.join(work_dir, old_video_name)
        # new_video_path = os.path.join(work_dir, new_video_name)
        #
        # os.rename(old_video_path, new_video_path)

        file_info = api.video.upload_paths(ds.id, [video_name], [video_path])


        a=0