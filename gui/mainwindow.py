import sys
from PyQt6 import QtWidgets, uic


class SolWatcherGUI:
    def __init__(self):
        app = QtWidgets.QApplication(sys.argv)
        self.ui = uic.loadUi("gui/form.ui")
        self.ui.show()
        app.exec()

