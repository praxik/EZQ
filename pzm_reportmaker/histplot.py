#!/usr/bin/env python
# a stacked bar plot with errorbars
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import sys

fname = sys.argv[1]
bins = eval(sys.argv[2])
vals = eval(sys.argv[3])
colors = eval(sys.argv[4])

# Test data
#fname = "test.svg"
#bins = [-500, -400, -300, -200, -100, 0, 100, 200, 300, 400, 500]
#vals = [8.286419069999974, 2.09940408, 3.3870682350000005, 5.421977910000001, 10.045559565000001, 15.816696840000002, 16.297068959999997, 14.342221304999999, 0.15790009500000005, 0, 0]
#colors = [[1.0, 0.0, 0.0, 0.85], [0.9686274509803922, 0.16862745098039217, 0.03529411764705882, 0.85], [0.9372549019607843, 0.33725490196078434, 0.07058823529411765, 0.85], [0.9058823529411765, 0.5058823529411764, 0.10588235294117647, 0.85], [0.8745098039215686, 0.6745098039215687, 0.1411764705882353, 0.85], [0.8470588235294118, 0.8470588235294118, 0.1803921568627451, 0.85], [0.7411764705882353, 0.807843137254902, 0.1411764705882353, 0.85], [0.6392156862745098, 0.7725490196078432, 0.10588235294117647, 0.85], [0.5372549019607843, 0.7372549019607844, 0.07058823529411765, 0.85], [0.43529411764705883, 0.7019607843137254, 0.03529411764705882, 0.85], [0.3333333333333333, 0.6666666666666666, 0.0, 0.85]]

rymax = reduce(lambda x, y: x+y, vals)
plt.ylim(0, rymax)
plt.xlim(-600, 600)
plt.ylabel('Acres', fontsize=30)
plt.xlabel('Profit ($)', fontsize=30)

# Force gridlines parallel to x-axis to make bar values easier to read
plt.grid(b=True, which='both', axis='y')
plt.tick_params(axis='both', labelsize=18)
plt.tick_params(axis='y', pad=10)
plt.bar(bins, vals, color=colors, width=100, align='center')

plt.xticks( bins, ('< -500', '-400', '-300', '-200', '-100',
  '0', '100', '200', '300', '400', '> 500'), rotation=60 )

plt.subplots_adjust(bottom=0.25,left=0.15)

plt.gca().yaxis.set_major_locator(MaxNLocator(prune='lower'))

#plt.savefig('hist.png',dpi=250) # 250 is about the right dpi if using raster
plt.savefig(fname)
#plt.show()
