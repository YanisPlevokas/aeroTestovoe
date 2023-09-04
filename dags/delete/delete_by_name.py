import os


def delete_files(**context):
    filename = context['templates_dict']['filename']
    os.remove(filename)
