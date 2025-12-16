import streamlit as st
import numpy as np
import cv2
from PIL import Image
import io
import zipfile
import os
import shutil
from spark_processor import SparkImageProcessor

st.set_page_config(
    page_title="Lab04 - BgRemover",
    layout="wide"
)

@st.cache_resource
def get_spark_processor():
    return SparkImageProcessor()

spark_processor = get_spark_processor()

INPUT_FOLDER = "input_images"
OUTPUT_FOLDER = "output_images"

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Header
st.title("Lab04 - BgRemover")
st.markdown("---")

# Initialize session state
if 'processed_images' not in st.session_state:
    st.session_state.processed_images = []
if 'original_images' not in st.session_state:
    st.session_state.original_images = []
if 'selected_images' not in st.session_state:
    st.session_state.selected_images = []
if 'current_session_id' not in st.session_state:
    st.session_state.current_session_id = None
if 'processing_results' not in st.session_state:
    st.session_state.processing_results = None

# Upload Section
col_title, col_clear = st.columns([4, 1])
with col_title:
    st.subheader("1. Upload Anh")
with col_clear:
    st.write("")
    if st.button("Clear", type="secondary"):
        st.session_state.processed_images = []
        st.session_state.original_images = []
        st.session_state.selected_images = []
        st.session_state.current_session_id = None
        st.session_state.processing_results = None
        st.rerun()

uploaded_files = st.file_uploader(
    "Chon cac file anh",
    type=['png', 'jpg', 'jpeg'],
    accept_multiple_files=True
)

if uploaded_files:
    st.info(f"Da chon {len(uploaded_files)} anh")
    
    # Process button
    st.markdown("###")
    st.subheader("2. Xu Ly")
    if st.button("Bat Dau Xu Ly", type="primary"):
        # Create new session
        session_id = spark_processor.create_session_id()
        st.session_state.current_session_id = session_id
        
        # Create session folders
        session_input_folder = os.path.join(INPUT_FOLDER, session_id)
        os.makedirs(session_input_folder, exist_ok=True)
        
        # Save uploaded files to input folder
        status_text = st.empty()
        status_text.text("Dang luu anh...")
        
        for uploaded_file in uploaded_files:
            file_path = os.path.join(session_input_folder, uploaded_file.name)
            with open(file_path, 'wb') as f:
                f.write(uploaded_file.getbuffer())
        
        # Process with Spark
        status_text.text("Dang xu ly voi Spark...")
        
        with st.spinner("Dang xu ly..."):
            results = spark_processor.process_images_batch(
                INPUT_FOLDER, 
                OUTPUT_FOLDER, 
                session_id
            )
        
        st.session_state.processing_results = results
        
        # Load processed images for display
        st.session_state.processed_images = []
        st.session_state.original_images = []
        st.session_state.selected_images = []
        
        session_output_folder = os.path.join(OUTPUT_FOLDER, session_id)
        
        for input_path, output_path, status in results:
            if status == "Success" and output_path and os.path.exists(output_path):
                # Load original
                original_img = cv2.imread(input_path)
                original_rgb = cv2.cvtColor(original_img, cv2.COLOR_BGR2RGB)
                
                # Load processed
                processed_img = cv2.imread(output_path)
                processed_rgb = cv2.cvtColor(processed_img, cv2.COLOR_BGR2RGB)
                
                filename = os.path.basename(input_path)
                
                st.session_state.original_images.append({
                    'name': filename,
                    'image': original_rgb,
                    'path': input_path
                })
                st.session_state.processed_images.append({
                    'name': filename,
                    'image': processed_rgb,
                    'path': output_path
                })
                st.session_state.selected_images.append(True)
        
        status_text.text("")
        st.success(f"Hoan thanh! Da xu ly {len(results)} anh")

# Display results
if st.session_state.processed_images:
    st.markdown("---")
    st.subheader("3. Ket Qua")
    
    # Display images
    for idx, (original_data, processed_data) in enumerate(zip(st.session_state.original_images, st.session_state.processed_images)):
        st.write(f"**{processed_data['name']}**")
        
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            st.write("Anh goc")
            st.image(original_data['image'], width=250)
        with col2:
            st.write("Da xu ly")
            st.image(processed_data['image'], width=250)
        with col3:
            st.write("")
            st.write("")
            st.session_state.selected_images[idx] = st.checkbox(
                "Chon",
                value=st.session_state.selected_images[idx],
                key=f"select_{idx}"
            )
        
        st.markdown("---")
    
    # Download options
    selected_count = sum(st.session_state.selected_images)
    
    if selected_count > 0:
        st.write(f"Da chon {selected_count} anh")
        
        col1, col2 = st.columns(2)
        with col1:
            download_format = st.selectbox("Dinh dang", ["PNG", "JPEG"])
        with col2:
            st.write("")
            st.write("")
            if st.button("Tai Xuong ZIP", type="primary"):
                zip_buffer = io.BytesIO()
                
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for idx, is_selected in enumerate(st.session_state.selected_images):
                        if is_selected:
                            processed_data = st.session_state.processed_images[idx]
                            
                            pil_image = Image.fromarray(processed_data['image'])
                            
                            img_buffer = io.BytesIO()
                            if download_format == "PNG":
                                pil_image.save(img_buffer, format='PNG')
                                file_ext = '.png'
                            else:
                                if pil_image.mode == 'RGBA':
                                    pil_image = pil_image.convert('RGB')
                                pil_image.save(img_buffer, format='JPEG', quality=95)
                                file_ext = '.jpg'
                            
                            img_buffer.seek(0)
                            
                            original_name = processed_data['name'].rsplit('.', 1)[0]
                            new_name = f"{original_name}_no_bg{file_ext}"
                            zip_file.writestr(new_name, img_buffer.getvalue())
                
                zip_buffer.seek(0)
                
                st.download_button(
                    label="Click de tai",
                    data=zip_buffer,
                    file_name="processed_images.zip",
                    mime="application/zip"
                )
    else:
        st.warning("Vui long chon it nhat mot anh")
