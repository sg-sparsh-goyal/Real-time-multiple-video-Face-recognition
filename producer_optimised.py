import cv2
import os
import time
import base64
import json
import numpy as np
from kafka import KafkaProducer
from multiprocessing import Process

def produce_video_frames(video_path, kafka_broker, topic):
    producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    cap = cv2.VideoCapture(video_path)
    frame_count = 0
    video_file = os.path.basename(video_path)

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
        
        resized_frame = cv2.resize(frame, (640, 360))  # Reduce size for efficiency
        _, buffer = cv2.imencode('.jpg', resized_frame)
        encoded_frame = base64.b64encode(buffer.tobytes()).decode('utf-8')

        message = {
            "video_file": video_file,
            "frame_count": frame_count,
            "frame": encoded_frame
        }
        producer.send(topic, value=message)
        frame_count += 1
        time.sleep(0.03)  # Adjust delay as needed

    cap.release()
    producer.close()

def main(folder_path, kafka_broker, topic):
    processes = []

    for video_file in os.listdir(folder_path):
        if video_file.endswith(('.mp4', '.avi', '.mov', '.mkv')):
            video_path = os.path.join(folder_path, video_file)
            process = Process(target=produce_video_frames, args=(video_path, kafka_broker, topic))
            process.start()
            processes.append(process)

    for process in processes:
        process.join()

if __name__ == "__main__":
    folder_path = r'C:\Users\Administrator\Desktop\Sparsh\Kafka\sample_videos\Human'
    kafka_broker = 'localhost:9092'
    topic = 'video_frames'
    main(folder_path, kafka_broker, topic)
