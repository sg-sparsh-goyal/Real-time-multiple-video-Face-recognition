import cv2
import numpy as np
import torch
from kafka import KafkaConsumer
from multiprocessing import Process, Queue
import time
import os
from ultralytics import YOLO

model = YOLO("multiple_videos/yolov8m_200e.pt")

output_dir = r"C:\Users\Administrator\Desktop\Sparsh\Kafka\multiple_videos\Output_vids"

def consume_video_frames(topic, kafka_broker, stop_signal):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_broker,
        auto_offset_reset='latest',
        consumer_timeout_ms=1000
    )
    
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = None
    frame_width, frame_height = None, None
    
    while not stop_signal.empty():
        for message in consumer:
            frame = np.frombuffer(message.value, dtype=np.uint8)
            frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
            if frame is not None:
                if out is None:
                    frame_height, frame_width = frame.shape[:2]
                    out = cv2.VideoWriter(
                        f"{output_dir}/{topic}_result.mp4", 
                        fourcc, 20.0, 
                        (frame_width, frame_height)
                    )
                
                results = model.predict(frame, save=False, stream=True)
                
                for result in results:
                    frame = result.plot()

                out.write(frame)

                # Display the frame
                cv2.imshow(f"Video: {topic}", frame)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    stop_signal.put(True)
                    break
        if not stop_signal.empty():
            break

    cv2.destroyAllWindows()
    consumer.close()
    if out:
        out.release()

def main(kafka_broker, topics):
    processes = []
    stop_signal = Queue()

    for topic in topics:
        stop_signal.put(False)  # Initialize stop signal
        process = Process(target=consume_video_frames, args=(topic, kafka_broker, stop_signal))
        process.start()
        processes.append(process)

    try:
        while not stop_signal.empty():
            time.sleep(1)  
    except KeyboardInterrupt:
        stop_signal.put(True)
    
    for process in processes:
        process.join()

if __name__ == "__main__":
    kafka_broker = 'localhost:9092'
    topics = [
    'multiple_vid_face_2786540-sd_640_360_25fps.mp4',
    'multiple_vid_face_2795173-sd_640_360_25fps.mp4',
    'multiple_vid_face_2795399-sd_360_640_25fps.mp4',
    'multiple_vid_face_3209298-sd_640_360_25fps.mp4',
    'multiple_vid_face_3249935-sd_640_360_25fps.mp4',
    'multiple_vid_face_3252424-sd_640_360_25fps.mp4',
    'multiple_vid_face_852364-sd_640_360_24fps.mp4',
    'multiple_vid_face_852415-sd_640_360_24fps.mp4',
    'multiple_vid_face_855564-sd_640_360_24fps.mp4',
    'multiple_vid_face_855574-sd_640_360_25fps.mp4',
    'multiple_vid_face_b.mp4'
    ]
    main(kafka_broker, topics)

