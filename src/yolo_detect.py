from pathlib import Path
import sys

# Structure Path
current_file_path = Path(__file__).resolve()
project_root = current_file_path.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import os
from scripts.constants import IMG_FILE_PATH, YOLO_OUTPUT_CSV
from ultralytics import YOLO
import pandas as pd

# Load the lightweight model
model = YOLO("yolov8n.pt")

# Define COCO class IDs (YOLO standard mapping)
# 0 = person, 39 = bottle, 41 = cup, etc.
# Treat bottles, cups, and bowls as "Medical/Cosmetic Products"
PRODUCT_CLASSES = [39, 41, 45, 76]  # bottle, cup, bowl, vase (often used for packaging)


def classify_image(detections):
    """
    Classify based on document rules :
    - Promotional: Person + Product
    - Product Display: Product, no Person
    - Lifestyle: Person, no Product
    - Other: Neither
    """
    has_person = False
    has_product = False

    # Iterate through all detected objects in the image
    for result in detections:
        boxes = result.boxes
        for box in boxes:
            cls_id = int(box.cls[0])  #  class ID (0, 39, etc.)

            if cls_id == 0:
                has_person = True
            elif cls_id in PRODUCT_CLASSES:
                has_product = True

    # Apply Logic
    if has_person and has_product:
        return "promotional"
    elif has_product and not has_person:
        return "product_display"
    elif has_person and not has_product:
        return "lifestyle"
    else:
        return "other"


def process_images():
    results_list = []

    for root, dirs, files in os.walk(IMG_FILE_PATH):
        for file in files:
            if file.lower().endswith((".jpg", ".jpeg", ".png")):
                image_path = os.path.join(root, file)

                try:
                    message_id = os.path.splitext(file)[0]
                    channel_name = os.path.basename(root)
                except:
                    continue

                try:
                    results = model(image_path, conf=0.25, verbose=False)

                    category = classify_image(results)
                    # Get best confidence score (highest in the image)
                    if results[0].boxes:
                        best_conf = float(results[0].boxes.conf.max())
                    else:
                        best_conf = 0.0

                    results_list.append(
                        {
                            "message_id": message_id,
                            "channel_name": channel_name,
                            "detected_class": category,
                            "confidence_score": best_conf,
                        }
                    )
                    print(f"Processed {message_id}: {category}")

                except Exception as e:
                    print(f"Error processing {image_path}: {e}")

    # 4. Save to CSV [cite: 264]
    df = pd.DataFrame(results_list)
    df.to_csv(YOLO_OUTPUT_CSV, index=False)
    print(f"Done! Results saved to {YOLO_OUTPUT_CSV}")


if __name__ == "__main__":
    process_images()
