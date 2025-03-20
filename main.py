from firebase_admin import initialize_app, firestore, credentials, storage
import time
import calendar
import tempfile
import traceback
import requests
import pysrt
import subprocess
import os
import ffmpeg
from PIL import Image
from io import BytesIO
import cv2
import json
import random
import base64


data_json_encoded = os.getenv('DATA_JSON', '{}')
data_json = base64.b64decode(data_json_encoded).decode()
data = json.loads(data_json)
print("input payload is: ",type(data),data)
print("type : ",type(data["data"]))
print("type : ",type(data["data"]["input"]["Engine"]["credentials"]))
firebaseCredentials = requests.get("https://imagesai.appypie.com/"+data["data"]["input"]["Engine"]["credentials"])
firebaseCredentialsJson = firebaseCredentials.json()
temp_file_path = ""
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".json") as temp_file:
    json.dump(firebaseCredentialsJson, temp_file, indent=4)
    temp_file_path = temp_file.name

cred = credentials.Certificate(temp_file_path)
initialize_app(cred,{'storageBucket' : "imagesai.appypie.com"})
db = firestore.client()
bucket = storage.bucket()



def getFirstFrameUrl(video_url,userid="00"):
    data = {}
    video_capture = cv2.VideoCapture(video_url)
    if not video_capture.isOpened():
        print("Error opening video stream or file")
        video_capture.release()
        return None
    success, frame = video_capture.read()
    if success:
        is_success, buffer = cv2.imencode(".png", frame)
        image_data = buffer.tobytes()
        gmt = time.gmtime()
        ts = calendar.timegm(gmt)
        blob_name = f"{userid}/firstFrameUrlThumbnailHitshit{ts}{random.randint(1000, 2000)}.png"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(image_data, content_type='image/png')

        download_url = (blob.public_url).replace("storage.googleapis.com/", "") 
        
        print("First frame extracted and saved as 'first_frame.jpg'")
    else:
        video_capture.release()
        return "Error reading the first frame"

    video_capture.release()
    return download_url

def pngtowebp(data):
    try:
        imagePIL = Image.open(BytesIO(requests.get(data["image"]).content)).convert("RGBA")

        byte_stream = BytesIO()
        imagePIL.save(byte_stream, format="WEBP", quality=data.get("quality",20))
        byte_stream.seek(0)

        ts = int(time.time())
        blob = bucket.blob(f'{data["userId"]}/PreviewImageUrlT{str(ts)}{str(random.randint(1000, 2000))}.webp')
        blob.upload_from_string(byte_stream.read(), content_type='image/png')
        return blob.public_url.replace("storage.googleapis.com/", "")
    except Exception as e:
        print(str(e))
        return data["image"]

def BqueryLog(payload,bucketId = False,method = ""):
  print("BqueryLogtest"+str(payload)+" "+str(bucketId)+" "+str(method))
  try:
    if bucketId and payload["Action"] == "trackAssetFailure":
        print("failedcaseLogQuery"+str(payload))
        gmt = time.gmtime()
        ts = calendar.timegm(gmt)
        db.collection('AiGenEmail').document(bucketId).set({
            "userId" : payload["Alias"],
            "method" : method,
            "completionTimestamp" : ts,
            "failed": {payload["taskId"] : payload["taskUrl"]}
        }, merge=True)
        db.collection('CreditLog').document(bucketId).set({
            "failed": {payload["taskId"] : payload["taskUrl"]}
        }, merge=True)
        print("yeyyfailed "+str(payload)+" "+str(bucketId)+" "+str(method))
    elif bucketId and payload["Action"] == "trackAssetCreation":
        db.collection('AiGenEmail').document(bucketId).set({
            "succeeded": {payload["taskId"] : payload["taskUrl"]}
        }, merge=True)
        db.collection('CreditLog').document(bucketId).set({
            "succeeded": {payload["taskId"] : payload["taskUrl"]}
        }, merge=True)
  except Exception as e:
    print("failedcaseLogQueryE"+str(e))
  try:
    if(payload["device"] != ""):
      response = requests.request("POST", "https://us-central1-appydesigne-24e6c.cloudfunctions.net/querylog", json=payload, timeout=1)
  except Exception as e:
    print("error entering data into bigquery: ", str(e))
  return "Done"


def postGenerationProcess(data):
    # document(data["imageId"]).collection(data["userid"]).document(data["uniqueId"]
    # data = request.get_json() 
    main_ref = db.collection('StudioQueueData').document(data["data"]["imageId"])
    try:
        preprocessed = data.get("preprocessed",True)
        if preprocessed:
            bucket_ref = db.collection('StudioQueueData').document(data["data"]["imageId"]).collection(data["data"]["userid"]).document(data["data"]["uniqueId"])
            bucketData = bucket_ref.get().to_dict()
            generationRequests = bucketData.get("videoRequestsCount",0)
            count = generationRequests - 1
            bucket_ref.update({"videoRequestsCount": count })
            if not data["error"]:
                print(f"postGenerationProcess,{data},{type(data)}")
                project_ref = db.collection('StudioQueueData').document(data["data"]["imageId"]).collection(data["data"]["userid"]).document(data["data"]["uniqueId"]).collection('videos').document(data["videoIndex"])
                project_ref.set({
                    "video_url": data["video_url"]["file_urls"],
                    "subtitles" : data["subtitles"],
                    "music_url" : data["music_url"],
                    "speech_url" : data["speech_url"],
                    "transition" : data["data"]["input"]["Engine"]["instructions"][int(data["videoIndex"])].get("transition","none"),
                    "error" : False
                }, merge=True)
            else:
                project_ref = db.collection('StudioQueueData').document(data["data"]["imageId"]).collection(data["data"]["userid"]).document(data["data"]["uniqueId"]).collection('videos').document(data["videoIndex"])
                project_ref.set({
                    "reason" : data["reason"],
                    "error" : True
                }, merge=True)
                raise ValueError(data["reason"])
            time.sleep(1)
        #handling last request 
        bucketData = bucket_ref.get().to_dict()
        mainData = main_ref.get().to_dict()
        if(bucketData["videoRequestsCount"]<=0 and not mainData["isGenerationDone"]):
            video_urls = []
            transitions = []
            if preprocessed:
                project_ref = db.collection('StudioQueueData').document(data["data"]["imageId"]).collection(data["data"]["userid"]).document(data["data"]["uniqueId"]).collection('videos')
                docs = project_ref.stream()
                for doc in docs:
                    doc_dict = doc.to_dict()
                    video_url = doc_dict.get("video_url", "No video_url found")
                    video_urls.append(video_url)
                    transitions.append(doc_dict.get("transition", "fade"))
            else:
                video_urls = data["video_url"]
                index = 0
                for video in data["data"]["input"]["Engine"]["videos"]:
                    transitions.append(data["data"]["input"]["Engine"]["instructions"][index]["transition"])
                    index = index+1
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    videos = []
                    count = 0
                    for video_url in video_urls:
                        response = requests.get(video_url, stream=True)
                        video_path = os.path.join(temp_dir, f"video_{count}.mp4")
                        video_path_resized = os.path.join(temp_dir, f"video_resized_{count}.mp4")
                        with open(video_path, "wb") as f:
                            for chunk in response.iter_content(1024):
                                f.write(chunk)
                        count = count+1
                        subprocess.run(f'ffmpeg -i {video_path} -vf "scale={data["data"]["input"]["Engine"]["width"]}:{data["data"]["input"]["Engine"]["height"]}:force_original_aspect_ratio=decrease,pad={data["data"]["input"]["Engine"]["width"]}:{data["data"]["input"]["Engine"]["height"]}:(ow-iw)/2:(oh-ih)/2:black" {video_path_resized}  2>/dev/null', shell=True)
                        # subprocess.run(f'ffmpeg -i {video_path} -vf "scale={data["data"]["input"]["Engine"]["height"]}:{data["data"]["input"]["Engine"]["width"]}:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2:black" {video_path_resized} 2>/dev/null', shell=True)
                        videos.append(video_path_resized)
                    audio_files = []
                    subtitle_files = []
                    video_files_with_duration= []
                    count = 0
                    for video_path in videos:
                        audio_path = os.path.join(temp_dir, f"{video_path[:-4]}.mp3")
                        subtitle_path = os.path.join(temp_dir, f"{video_path[:-4]}.srt")
                        print(subtitle_path)

                        # cmd_duration=f'ffprobe -i "{video_path}" -show_entries format=duration -v quiet -of csv="p=0" 2>/dev/null'
                        # duration = subprocess.getoutput(cmd_duration)
                        # def get_video_duration(video_path):

                        try:
                            cmd = ['ffprobe', '-i', video_path, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv=p=0']
                            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                            duration = float(result.stdout.strip()) if result.stdout.strip() else 0
                        except (subprocess.CalledProcessError, ValueError):
                            duration = 0
                        # try:
                        #     cmd = [
                        #         'ffmpeg', '-i', video_path, '-f', 'null', '-'
                        #     ]
                        #     result = subprocess.run(cmd, stderr=subprocess.PIPE, text=True)
                            
                        #     # Extract duration from stderr
                        #     import re
                        #     match = re.search(r"Duration: (\d+):(\d+):(\d+\.\d+)", result.stderr)
                        #     if match:
                        #         h, m, s = map(float, match.groups())
                        #         duration = h * 3600 + m * 60 + s
                        #     else:
                        #         duration = 0
                        # except Exception:
                        #     duration = 0

                        print(f"{video_path} duration is {duration}")

                        cmd_audio = f'ffmpeg -y -i "{video_path}" -vn -q:a 2 -acodec libmp3lame "{audio_path}" 2>/dev/null' if subprocess.run(
                            f'ffprobe -i "{video_path}" -show_streams -select_streams a -loglevel error 2>/dev/null', shell=True, stdout=subprocess.PIPE
                        ).stdout else f'ffmpeg -y -f lavfi -i anullsrc=r=44100:cl=stereo -t {duration} -acodec libmp3lame -q:a 9 "{audio_path}" 2>/dev/null'

                        subprocess.run(cmd_audio, shell=True)

                        if subprocess.run(f'ffprobe -i "{video_path}" -show_streams -select_streams s -loglevel error 2>/dev/null', shell=True, stdout=subprocess.PIPE).stdout:
                            subprocess.run(f'ffmpeg -y -i "{video_path}" -map 0:s:0 "{subtitle_path}" 2>/dev/null', shell=True)
                            print(f"Subtitles extracted to {subtitle_path}")
                        else:
                            open(subtitle_path, "w").close()
                            print(f"No subtitles found. Empty file created at {subtitle_path}")

                        audio_files.append(audio_path)
                        subtitle_files.append({"path" : subtitle_path, "duration" : duration})
                        video_files_with_duration.append({"path" : video_path, "duration" : duration,"transition" : transitions[count]})
                        count = count +1
                        
                    audio_txtfile_list = os.path.join(temp_dir, "mergedAudio.txt")
                    merged_audio_file = os.path.join(temp_dir, "mergedAudio.mp3")

                    with open(audio_txtfile_list, "w") as f:
                        for file in audio_files:
                            f.write(f"file '{file}'\n")
                    
                    subprocess.run(f'ffmpeg -y -f concat -safe 0 -i {audio_txtfile_list} -c copy {merged_audio_file} 2>/dev/null', shell=True)
                    

                    # for file in audio_files:
                    #     if os.path.exists(file):
                    #         shutil.copy(file, destination_dir)
                    
                    merged_subtitle_file = os.path.join(temp_dir, "mergedSubtitles.srt")
                    subtitleStartTime = 0
                    mergedSubs = []
                    for file in subtitle_files:
                        subs = pysrt.open(file["path"])
                        for s in subs:
                            s.start.shift(seconds=subtitleStartTime)
                            s.end.shift(seconds=subtitleStartTime)
                            mergedSubs.append(s)
                        subtitleStartTime+=float(file["duration"])
                    mergedSubs = pysrt.SubRipFile(mergedSubs)
                    if not mergedSubs:
                        mergedSubs.append(
                            pysrt.SubRipItem(1, start=pysrt.SubRipTime(0, 0, 0), end=pysrt.SubRipTime(0, 0, 1), text="\u200B")
                        )
                    mergedSubs.save(merged_subtitle_file, encoding='utf-8')
                    

                    merged_Video_file = os.path.join(temp_dir, "mergedVideo.mp4")
                    video_txtfile_list = os.path.join(temp_dir, "mergedVideo.txt")

                    if data["data"]["input"]["Engine"].get("transition","not needed")=="none":
                        with open(video_txtfile_list, "w") as f:
                            for video in videos:
                                f.write(f"file '{os.path.abspath(video)}'\n")
                        subprocess.run(f'ffmpeg -y -f concat -safe 0 -i {video_txtfile_list} -an -sn -c copy {merged_Video_file} 2>/dev/null', shell=True)
                    else:
                        input_cmd = []
                        filters = []
                        # transition=data["data"]["input"]["Engine"]["transition"]
                        transistion_duration=1
                        offset = 0
                        print(video_files_with_duration)
                        for i, video in enumerate(video_files_with_duration):
                            input_cmd.extend(["-i", video["path"]])
                            if i > 0:
                                if i==1:
                                    filters.append(f"[{i-1}:v][{i}:v]xfade=transition={video['transition']}:duration={transistion_duration}:offset={offset}[v{i}];")
                                else:
                                    filters.append(f"[v{i-1}][{i}:v]xfade=transition={video['transition']}:duration={transistion_duration}:offset={offset}[v{i}];")
                            offset = offset + float(video["duration"])-1
                                

                        filter_complex = "".join(filters).rstrip(";")
                        final_map = f"[v{len(videos) - 1}]"

                        cmd = ["ffmpeg", "-y", "-loglevel", "error"] + input_cmd + ["-filter_complex", filter_complex, "-map", final_map, "-an", "-sn", "-c:v", "libx264", merged_Video_file]



                        print("Executing command:\n", " ".join(cmd))
                        result = subprocess.run(cmd, text=True)
                    
                    final_output = os.path.join(temp_dir, f"overLappedVideo.mp4")
                    subprocess.run(f'ffmpeg -y -i {merged_Video_file} -i {merged_audio_file} -i {merged_subtitle_file} -c:v copy -c:a aac -c:s mov_text {final_output} 2>/dev/null', shell=True)
                    gmt = time.gmtime()
                    ts = calendar.timegm(gmt)
                    blob = bucket.blob(f'{data["data"]["userid"]}/videoMerger{str(ts)}{str(random.randint(1000, 2000))}.mp4')
                    with open(final_output, "rb") as outputFile:
                        blob.upload_from_file(outputFile, content_type="video/mp4")

                    imageUrlOutput = (blob.public_url).replace("storage.googleapis.com/", "")
                    
                    thumbnailOutput = pngtowebp({"image" : getFirstFrameUrl(imageUrlOutput,data["data"]["userid"]), "userId" : data["data"]["userid"]})
                    
                    ref = db.collection('StudioQueueData').document(data["data"]["imageId"]).collection(data["data"]["userid"]).document(data["data"]["uniqueId"])
                    ak = ref.get().to_dict()
                    ak["ImageUrl"] = imageUrlOutput
                    ak["PreviewImage"] = imageUrlOutput
                    ak["isPlaceholder"] = False
                    ak["thumbnail"] = thumbnailOutput
                    ak["downloadableData"] = imageUrlOutput
                    # ak["embedding"] = generateImageEmbedding(thumbnailOutput)
                    # ak["promptembedding"] = generateTextEmbeddingNew(data["input"]["prompt"])
                    ref.update({
                        'ImageUrl': imageUrlOutput,
                        'thumbnail' : thumbnailOutput,
                        'errorGenerating' :   False,
                        'isPlaceholder' : False,
                        'PreviewImage' : imageUrlOutput
                    })
                    db.collection('userData').document(data["data"]["userid"]).collection("AiStudio").document(data["data"]["uniqueId"]).set(ak)
                    BqueryLog(
                        {
                            "Action" : "trackAssetCreation",
                            "Alias" : data["data"]["userid"],
                            "taskId" : data["data"]["uniqueId"],
                            "taskPrompt" : "",
                            "taskUrl" : imageUrlOutput,
                            "credits" : data["data"]["input"].get("creditNeedForGen" , 0),
                            "device" : data["data"]["input"].get("device" , ""),
                            "country" : data["data"]["input"].get("country" , ""),
                            "plan" : data["data"]["input"].get("paymentPlan" , ""),
                            "assetType" : "AI VIDEOS MERGE"
                        },
                        data["data"]["imageId"],
                        data["data"].get("method","")
                    )
            except Exception as e:
                raise ValueError(str(e))
            main_ref.update({"isGenerationDone": True, "generationRequests" : 0 })
    except Exception as e:
        traceback.print_exc()
        ref = db.collection('StudioQueueData').document(data["data"]["imageId"]).collection(data["data"]["userid"]).document(data["data"]["uniqueId"])
        reason = data.get("reason",str(e))
        ref.update({
            'ImageUrl': reason,
            'errorGenerating' :   True,
            'isGenerationDone': True,
            "isPlaceholder" : False
        })
        BqueryLog({
                "Action" : "trackAssetFailure",
                "Alias" : data["data"]["userid"],
                "taskId" : data["data"]["uniqueId"],
                "taskPrompt" : "",
                "taskUrl" : reason,
                "credits" : data["data"]["input"].get("creditNeedForGen" , 0),
                "device" : data["data"]["input"].get("device" , ""),
                "country" : data["data"]["input"].get("country" , ""),
                "plan" : data["data"]["input"].get("paymentPlan" , ""),
                "assetType" : "AI VIDEOS MERGE"
            },
            data["data"]["imageId"],data["data"].get("method","")
        )

        main_ref.update({"isGenerationDone": True, "generationRequests" : 0 })
        
    return "Done"

# if __name__=="__main__":
postGenerationProcess(data)
