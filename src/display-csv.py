#!/usr/bin/env python
from matrixbase import MatrixBase
import time
from PIL import ImageColor
import pandas as pd

class DisplayCSV(MatrixBase):
    def __init__(self, *args, **kwargs):
        super(DisplayCSV, self).__init__(*args, **kwargs)

    def run(self):
        offset_canvas = self.matrix.CreateFrameCanvas()

        # startup animation
        statuscode_led_mapping = pd.read_csv('statuscode_led_mapping.csv', sep=";")
        # randomise order
        statuscode_led_mapping = statuscode_led_mapping.sample(frac=1).reset_index(drop=True)

        number_of_statuscode_rows = len(statuscode_led_mapping)

        # light every statuscode with delay to create cool animation
        for i in range(number_of_statuscode_rows):
            # fill all leds with black
            offset_canvas.Fill(0,0,0)

            # iterate over subsets of increasing size to light all statuscodes after another
            statuscode_led_mapping_subset = statuscode_led_mapping[0:i+1]

            # for every row of the current subset, set led color to white
            for j, statuscode_led_mapping_row in statuscode_led_mapping_subset.iterrows():
                led_mapping_string = statuscode_led_mapping_row['leds']
                leds_xy = led_mapping_string.split("&")
                for led_xy in leds_xy:
                    x, y = led_xy.split("-")
                
                    offset_canvas.SetPixel(int(x), int(y), 255, 255, 255)

            offset_canvas = self.matrix.SwapOnVSync(offset_canvas)
            #time.sleep(0.3 - 0.3 * (i / number_of_statuscode_rows))
            time.sleep(0.01 + (1 / (i+1)**2))

        time.sleep(1)


        # loop

        while True:

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
           

# Main function
if __name__ == "__main__":
    simple_square = DisplayCSV()
    if (not simple_square.process()):
        simple_square.print_help()
