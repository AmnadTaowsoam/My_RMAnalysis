# My_RMAnalysis
Create Database for RM analysis

## github.com
open Command line

    git clone https://github.com/AmnadTaowsoam/My_RMAnalysis.git

## directory
    mkdir documents
    cd documents
    mkdir rmanalysis
    mkdir rmanalysis_complete
    mkdir rmanalysis_pending
## environment 
command line:

    python -m venv env
    env\Scripts\activate.bat

requirements:

    pip install -r requirements.txt

## file rmanalysis
1. เอาไฟล์ผลวิเคราะห์มาใส่ใน Folder:documents/rmanalysis

## run Application
command line: จะทำการสร้าง Database_Buffer ก่อนที่จะมีการ Load เข้า Database จริง

    python main.py

command line: Load เข้า Database จริง

    python upload.py
    

