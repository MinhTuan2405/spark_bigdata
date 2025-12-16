import os
import cv2
import numpy as np
from pyspark import SparkContext, SparkConf
from background_remover import remove_background
from datetime import datetime
import uuid

def process_single_image(image_info):
    """Process a single image - to be used in Spark map operation"""
    input_path, output_path = image_info
    
    try:
        # Read image
        image = cv2.imread(input_path)
        if image is None:
            return (input_path, None, f"Failed to read image")
        
        # Convert BGR to RGB
        image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        
        # Process image
        processed_image = remove_background(image_rgb)
        
        # Convert back to BGR for saving
        processed_bgr = cv2.cvtColor(processed_image, cv2.COLOR_RGB2BGR)
        
        # Save processed image
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        cv2.imwrite(output_path, processed_bgr)
        
        return (input_path, output_path, "Success")
    except Exception as e:
        return (input_path, None, str(e))

class SparkImageProcessor:
    def __init__(self, app_name="BackgroundRemover"):
        """Initialize Spark context for image processing"""
        spark_master = os.getenv('SPARK_MASTER_URL', 'local[*]')
        
        conf = SparkConf().setAppName(app_name).setMaster(spark_master)
        conf.set("spark.driver.host", "streamlit-app")
        conf.set("spark.driver.bindAddress", "0.0.0.0")
        
        self.sc = SparkContext.getOrCreate(conf=conf)
        self.sc.setLogLevel("ERROR")
        self.sc.addPyFile("/app/background_remover.py")
        self.sc.addPyFile("/app/spark_processor.py")
    
    def create_session_id(self):
        """Create unique session ID with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"
    
    def process_images_batch(self, input_folder, output_folder, session_id):
        # Create session folders
        session_input = os.path.join(input_folder, session_id)
        session_output = os.path.join(output_folder, session_id)
        
        os.makedirs(session_input, exist_ok=True)
        os.makedirs(session_output, exist_ok=True)
        
        # Get all image files from session input folder
        image_files = []
        for filename in os.listdir(session_input):
            if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                input_path = os.path.join(session_input, filename)
                output_path = os.path.join(session_output, filename)
                image_files.append((input_path, output_path))
        
        if not image_files:
            return []
        
        image_rdd = self.sc.parallelize(image_files)
        results = image_rdd.map(process_single_image).collect()
        
        return results
    
    def stop(self):
        """Stop Spark context"""
        if self.sc:
            self.sc.stop()
    
    def get_session_history(self, input_folder, output_folder):
        sessions = []
        
        if not os.path.exists(input_folder):
            return sessions
        
        for session_id in os.listdir(input_folder):
            session_path = os.path.join(input_folder, session_id)
            if os.path.isdir(session_path):
                # Count images
                input_images = [f for f in os.listdir(session_path) 
                               if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
                
                output_path = os.path.join(output_folder, session_id)
                output_images = []
                if os.path.exists(output_path):
                    output_images = [f for f in os.listdir(output_path) 
                                   if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
                
                # Get timestamp from session_id
                timestamp_str = session_id.split('_')[0] + '_' + session_id.split('_')[1]
                try:
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    time_str = timestamp.strftime("%d/%m/%Y %H:%M:%S")
                except:
                    time_str = session_id
                
                sessions.append({
                    'session_id': session_id,
                    'timestamp': time_str,
                    'input_count': len(input_images),
                    'output_count': len(output_images),
                    'input_path': session_path,
                    'output_path': output_path
                })
        
        sessions.sort(key=lambda x: x['session_id'], reverse=True)
        return sessions
