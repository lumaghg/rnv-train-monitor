#!/usr/bin/env python
from matrixbase import MatrixBase
import time
from PIL import ImageColor
import pandas as pd
import numpy as np


def getHexColorForLine(line):
    if line == 5:
        return '00975F'
    if line == 21:
        return 'E30613'
    if line == 22:
        return 'FDC300'
    if line == 23:
        return 'E48F00'
    if line == 24:
        return '8D2176'
    if line == 25:
        return '9D9D9C'
    if line == 26:
        return 'F39B9B'
    return 'FFFFFF'

class DisplayCSV(MatrixBase):
    def __init__(self, *args, **kwargs):
        super(DisplayCSV, self).__init__(*args, **kwargs)





    def run(self):
        offset_canvas = self.matrix.CreateFrameCanvas()

        statuscode_led_mapping = pd.read_csv('statuscode_led_mapping.csv', sep=";")
        # randomise order
        statuscode_led_mapping = statuscode_led_mapping.sample(frac=1).reset_index(drop=True)


        # startup animation

        # create led_matrix dataframe with all led colors set to black
        led_matrix = pd.DataFrame(np.full((32,64), "000000"))

        number_of_statuscode_rows = len(statuscode_led_mapping)

        # light every statuscode with delay to create cool animation
        for i, statuscode_led_mapping_row in statuscode_led_mapping.iterrows():
            # add current row to the led matrix df
        
            led_mapping_string = statuscode_led_mapping_row['leds']
            leds_xy = led_mapping_string.split("&")
            for led_xy in leds_xy:
                x, y = led_xy.split("-")
            
                led_matrix.at[int(y), int(x)] = "FFFFFF"

            # display new led matrix df
            no_rows, no_columns = led_matrix.shape

            for y in range(0,no_rows):
                for x in range(0,no_columns):
                    color_hex = led_matrix.at[y,x]                    
                    color_rgb = ImageColor.getcolor(f"#{color_hex}", "RGB")
                    offset_canvas.SetPixel(int(x), int(y), color_rgb[0], color_rgb[1], color_rgb[2])      
            offset_canvas = self.matrix.SwapOnVSync(offset_canvas)



        time.sleep(2)


        #
        # show routes of each line
        lines = sorted(statuscode_led_mapping['line'].unique())

        print(lines)

        for line in lines:
            linecolor_hex = getHexColorForLine(line)

            led_mapping_rows_for_line = statuscode_led_mapping[statuscode_led_mapping['line'] == line]
            # set canvas black

            offset_canvas.Fill(0,0,0)

            for i, statuscode_led_mapping_row in led_mapping_rows_for_line.iterrows():

                # set pixels for current row
            
                led_mapping_string = statuscode_led_mapping_row['leds']
                leds_xy = led_mapping_string.split("&")
                for led_xy in leds_xy:
                    x, y = led_xy.split("-")  
                    color_rgb = ImageColor.getcolor(f"#{linecolor_hex}", "RGB")
                    offset_canvas.SetPixel(int(x), int(y), color_rgb[0], color_rgb[1], color_rgb[2])

            # display pixels
            offset_canvas = self.matrix.SwapOnVSync(offset_canvas)

            # delay between lines
            time.sleep(3)


        # loop

        while True:
            try:
                led_matrix = pd.read_csv('led-matrix.csv', header=None, dtype=str, index_col=None)

                # iterate over every cell
                no_rows, no_columns = led_matrix.shape

                for y in range(0,no_rows):
                    for x in range(0,no_columns):
                        color_hex = led_matrix.at[y,x]                    
                        color_rgb = ImageColor.getcolor(f"#{color_hex}", "RGB")
                        offset_canvas.SetPixel(x, y, color_rgb[0], color_rgb[1], color_rgb[2])      
                offset_canvas = self.matrix.SwapOnVSync(offset_canvas)

                time.sleep(2)
            except Exception:
                continue
           

# Main function
if __name__ == "__main__":
    simple_square = DisplayCSV()
    if (not simple_square.process()):
        simple_square.print_help()


    

