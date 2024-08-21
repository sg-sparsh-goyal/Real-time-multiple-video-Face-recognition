from ultralytics import YOLO

model = YOLO("multiple_videos/yolov8m_200e.pt")

source = r"C:\Users\Administrator\Desktop\Sparsh\Kafka\sample_videos\f.mp4"
output_dir = r"C:\Users\Administrator\Desktop\Sparsh\Kafka\multiple_videos\Output_vids"


results = model.predict(source, save=True, project=output_dir, name="result_video", show=True, stream=True)
# for r in results:
#     boxes = r.boxes  # Boxes object for bbox outputs
#     masks = r.masks  # Masks object for segment masks outputs
#     probs = r.probs  # Class probabilities for classification outputs

print("Video has been saved to:", output_dir)
