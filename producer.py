import cv2
import os
import time

import numpy as np
from kafka import KafkaProducer
from multiprocessing import Process, Queue

def produce_video_frames(video_path, kafka_broker, topic):
    producer = KafkaProducer(bootstrap_servers=kafka_broker)
    cap = cv2.VideoCapture(video_path)
    frame_count = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
        
        resized_frame = cv2.resize(frame, (300,300))
        _, buffer = cv2.imencode('.jpg', resized_frame)
        producer.send(topic, key=bytes(str(frame_count), 'utf-8'), value=buffer.tobytes())
        frame_count += 1
        time.sleep(0.03)  # Adjust this delay according to your requirements
    cap.release()
    producer.close()

def main(folder_path, kafka_broker, topic_prefix):
    processes = []

    for video_file in os.listdir(folder_path):
        if video_file.endswith(('.mp4', '.avi', '.mov', '.mkv')):
            video_path = os.path.join(folder_path, video_file)
            topic = f"{topic_prefix}_{video_file}"
            print(topic)
            process = Process(target=produce_video_frames, args=(video_path, kafka_broker, topic))
            process.start()
            processes.append(process)

    for process in processes:
        process.join()



if __name__ == "__main__":
    folder_path = r'C:\Users\Administrator\Desktop\Sparsh\Kafka\sample_videos\Human'
    kafka_broker = 'localhost:9092'
    topic_prefix = 'multiple_vid_face'
    main(folder_path, kafka_broker, topic_prefix)

