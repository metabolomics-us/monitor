import os


def add_agilent_file(tmp):
    print('\n-----  creating agilent file  -----')
    agi_folder = '%s/test.d' % tmp
    os.makedirs('%s/AcqData' % agi_folder)

    with open('%s/desktop.ini' % agi_folder, 'w') as agi_file:
        agi_file.write("blah")
    with open('%s/AcqData/AcqMethod.xml' % agi_folder, 'w') as agi_file:
        agi_file.write('method')
