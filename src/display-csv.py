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
