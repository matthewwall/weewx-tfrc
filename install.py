# installer for the weewx-tfrc driver
# Copyright 2019 Lucas Heijst, Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)

from setup import ExtensionInstaller

def loader():
    return TFRCInstaller()

class TFRCInstaller(ExtensionInstaller):
    def __init__(self):
        super(TFRCInstaller, self).__init__(
            version="0.1",
            name='tfrc',
            description='Capture data from tfrc',
            author="Matthew Wall",
            author_email="mwall@users.sourceforge.net",
            files=[('bin/user', ['bin/user/tfrc.py'])]
            )
