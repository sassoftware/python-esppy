#! /usr/bin/python3

import numpy as np
import cv2
import base64

def get_all_crops_from_bbox(_image_, _nObjects_,
                            _Object0_x, _Object0_y, _Object0_width, _Object0_height,
                            _Object1_x, _Object1_y, _Object1_width, _Object1_height,
                            _Object2_x, _Object2_y, _Object2_width, _Object2_height,
                            _Object3_x, _Object3_y, _Object3_width, _Object3_height,
                            _Object4_x, _Object4_y, _Object4_width, _Object4_height,
                            _Object5_x, _Object5_y, _Object5_width, _Object5_height):
    "Output: rx_min, rx_max, ry_min, ry_max, board_id, _board_image_"

    imageBufferBase64 = _image_
    numberOfObjects = _nObjects_

    # limit the max number of objects to 6
    if numberOfObjects > 6:        
        numberOfObjects = 6

    nparr = np.frombuffer(base64.b64decode(imageBufferBase64), dtype=np.uint8)    
    img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    #img_np = cv2.resize(img_np, (int(outWidth), int(outHeight)), cv2.INTER_LINEAR)

    image_h, image_w, _ = img_np.shape

    local_vars=locals()
    row={}
    for i in range(int(float(numberOfObjects))):
        row['_Object'+str(i)+'_x'] = local_vars['_Object'+str(i)+'_x']
        row['_Object'+str(i)+'_y'] = local_vars['_Object'+str(i)+'_y']
        row['_Object'+str(i)+'_width'] = local_vars['_Object'+str(i)+'_width']
        row['_Object'+str(i)+'_height'] = local_vars['_Object'+str(i)+'_height']

    #print(row)

    rx_min_list = []
    rx_max_list = []
    ry_min_list = []
    ry_max_list = []
    crop_list = []
    board_id_list = []
    
    for i in range(0, int(float(numberOfObjects))):
        #obj = row['_Object' + str(i) + '_']
        #prob = float(row['_P_Object' + str(i) + '_'])
        #probability = "(" + str(round(prob * 100, 1)) + "%)"
        x = float(row['_Object' + str(i) + '_x'])
        y = float(row['_Object' + str(i) + '_y'])
        width = float(row['_Object' + str(i) + '_width'])
        height = float(row['_Object' + str(i) + '_height'])

        rx_min = x - width  / 2
        rx_max = x + width  / 2
        ry_min = y - height / 2
        ry_max = y + height / 2
        
        x_min = int(image_w * rx_min)
        x_max = int(image_w * rx_max)
        y_min = int(image_h * ry_min)
        y_max = int(image_h * ry_max)
        
        # CROP ORIGINAL IMAGE
        new_crop = img_np[y_min:y_max, x_min:x_max, :]
        #all_crops.append(new_crop_img)

        if new_crop.size > 0:
            retval, nparr_crop = cv2.imencode(".JPEG", new_crop)

            img_blob_crop = np.array(nparr_crop).tostring()
            
            img_crop_base64 = base64.b64encode(img_blob_crop)

            #print(img_crop_base64)
            rx_min_list.append(rx_min)
            rx_max_list.append(rx_max)
            ry_min_list.append(ry_min)
            ry_max_list.append(ry_max)
            board_id_list.append(i)
            crop_list.append(img_crop_base64.decode('utf-8'))

    if len(board_id_list) == 0:
        return None, None, None, None, 0, None
    
    return rx_min_list, rx_max_list, ry_min_list, ry_max_list, board_id_list, crop_list
    


