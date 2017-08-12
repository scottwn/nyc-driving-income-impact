import shapefile
import json
shapes = shapefile.Reader('cb_2016_36_puma10_500k.shp').shapeRecords()
f = open('nyc_shapes2', 'w')
for shape in shapes:
    if 'Community' in shape.record[4]:
        f.write(shape.record[1])
        f.write(';');
        f.write(shape.record[4])
        f.write(';{\"type\":\"Polygon\", \"coordinates\":')
        f.write(json.dumps(shape.shape.points))
        f.write('}\n')
f.close()
