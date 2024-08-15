from fileinput import filename

from fastapi import FastAPI, UploadFile, File
from secrets import token_hex
from dotenv import load_dotenv
import os
import uvicorn
from db import Data
from fastapi.middleware.trustedhost import TrustedHostMiddleware

app = FastAPI( )

load_dotenv()


@app.get("/data/trunc-table")
async def trunc_table():
    data = Data()
    msg = "Table not truncated"
    code = 401
    if data.truncate_table():
        msg = "Table truncated"
        code = 200
    return {"message": msg}, code

@app.get("/data/import-data")
async def insert_data():
    data = Data()
    msg = "Data not inserted"
    code = 401
    tempfolder = os.getenv('TEMPFOLDER', '')
    if tempfolder == '':
        return {"success": False, "message": "Temp folder not set"}

    for file in os.listdir(tempfolder):
        file_path = os.path.join(tempfolder, file)
        if data.import_data(file_path):
            msg = "Data inserted"
            code = 200
            break

    return {"message": msg}, code

@app.get("/data/load-part1")
async def load_part1():
    data = Data()
    msg = "Part1 not loaded"
    code = 401
    rows = data.load_part1()
    if len(rows) > 0:
        msg = "Part1 loaded"
        code = 200
    return {"message": msg, "data": rows}, code

@app.get("/data/load-part2")
async def load_part2():
    data = Data()
    msg = "Part2 not loaded"
    code = 401
    rows = data.load_part2()
    if len(rows) > 0:
        msg = "Part2 loaded"
        code = 200
    return {"message": msg, "data": rows}, code


@app.post("/data/process-data")
async def process_data():
    data = Data()
    rslt = data.process_data()
    data = None
    if rslt:
        return {"message": "Data Processed"}, 200
    else:
        return {"message": "Data not processed"}, 401


@app.post("/upload/{file_name}")
async def upload(file_name:str, file: UploadFile = File(...)):
    filnam = file_name
    tempfolder = os.getenv('TEMPFOLDER', '')
    if tempfolder == '':
        return {"success": False, "message": "Temp folder not set"}


    file_path = os.path.join(tempfolder, filnam)
    with open(file_path, "wb") as file_object:
        content = await file.read()
        file_object.write(content)
    return {"success": True, "file_path": file_path, "message": "Upload Successful"}


@app.post("/upload/delete-all-files")
async def delete_all_files():
    tempfolder = os.getenv('TEMPFOLDER', '')
    if tempfolder == '':
        return {"success": False, "message": "Temp folder not set"}, 401

    for file in os.listdir(tempfolder):
        file_path = os.path.join(tempfolder, file)
        os.remove(file_path)
    return {"success": True, "message": "All files deleted"}, 200


@app.get("/")
async def root():
    return {"message": ""}, 200


if __name__ == "__main__":
    host = os.getenv('HOST','')
    port = os.getenv('PORT','')
    if host == '' or port == '':
        print("HOST or PORT not set in .env file")
        exit()

    uvicorn.run("app:app",
                host=os.getenv('HOST','localhost'),
                port=int(os.getenv('PORT',8000)),
                reload=os.getenv('RELOAD',False),
                log_level="info" )
