#!/usr/bin/env python
# a pie chart with legend
import matplotlib.pyplot as plt
import sys

def run():
  fname = sys.argv[1]
  # The following inputs are formatted as a python arrays,
  # so we just eval them directly.
  # Warning! This could be exploited to execute arbitrary code.
  labels = eval(sys.argv[2])
  values = eval(sys.argv[3])
  plot(values,labels)
  plt.savefig(fname,bbox_inches='tight')
  return


def plot(values,labels):
  colors = ['#FC9BBB', '#FC9BE4', '#DB68F2', '#B668F2', '#8F68F2', '#6884F2', '#68CBF2', '#68F2F0', '#68F2D9', '#68F271', '#D0F268', '#F2D768']
  # Set aspect ratio to be 1 so that pie is drawn as a circle.
  plt.axes(aspect=1)
  plt.pie(values,
          radius=1.5,
          colors=colors,
          shadow=False,
          startangle=90,
          # The following two options require matplotlib 1.4.0 or greater.
          # Ubuntu 14.04 is stuck at 1.3.1, so the newer matplotlib must be
          # installed via pip with: sudo pip install matplotlib --upgrade
          # If there are build problems, be sure to install package python2.7-dev
          counterclock=False,
          wedgeprops={'linewidth':1.5,'edgecolor':'white'}
         )

  plt.legend(labels,
             loc='center left',
             bbox_to_anchor=(1.2, 0.5),
             frameon=False,
             handlelength=1.5,
             handleheight=1.5,
             fontsize=18,
             labelspacing=0.1
            )
  plt.text( -1.5,1.5,
            'Expenses',
            fontdict={'fontsize':22}
            )
  #plt.savefig('pie_test.svg',bbox_inches='tight')
  #plt.show()
  return

run()
#plot([30,30,40],['One','two','three'])
#plot([68.0, 51.0, 57.95, 17.5, 13.5, 5.7, 6.3, 287.0, 29.25, 0.0, 10.0, 0.0],
#     ["Equipment", "Seed", "Fertilizer", "Herbicide/Insecticide", "Insurance", "Interest", "Grain Drying/\nHandling/\nHauling", "Land", "Labor", "Overhead/\nHome and Personal", "Other Inputs", "Custom"])
