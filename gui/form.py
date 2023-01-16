# Form implementation generated from reading ui file 'form.ui'
#
# Created by: PyQt6 UI code generator 6.4.0
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt6 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(800, 600)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.verticalLayout = QtWidgets.QVBoxLayout(self.centralwidget)
        self.verticalLayout.setObjectName("verticalLayout")
        self.widget = QtWidgets.QWidget(self.centralwidget)
        self.widget.setObjectName("widget")
        self.horizontalLayout = QtWidgets.QHBoxLayout(self.widget)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.label = QtWidgets.QLabel(self.widget)
        self.label.setObjectName("label")
        self.horizontalLayout.addWidget(self.label)
        self.label_price = QtWidgets.QLabel(self.widget)
        self.label_price.setObjectName("label_price")
        self.horizontalLayout.addWidget(self.label_price)
        spacerItem = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Minimum)
        self.horizontalLayout.addItem(spacerItem)
        self.label_3 = QtWidgets.QLabel(self.widget)
        self.label_3.setObjectName("label_3")
        self.horizontalLayout.addWidget(self.label_3)
        self.label_change = QtWidgets.QLabel(self.widget)
        self.label_change.setObjectName("label_change")
        self.horizontalLayout.addWidget(self.label_change)
        self.verticalLayout.addWidget(self.widget)
        self.tabWidget = QtWidgets.QTabWidget(self.centralwidget)
        self.tabWidget.setObjectName("tabWidget")
        self.tab = QtWidgets.QWidget()
        self.tab.setObjectName("tab")
        self.verticalLayout_2 = QtWidgets.QVBoxLayout(self.tab)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.graphicsView_24h = PlotWidget(self.tab)
        self.graphicsView_24h.setObjectName("graphicsView_24h")
        self.verticalLayout_2.addWidget(self.graphicsView_24h)
        self.tabWidget.addTab(self.tab, "")
        self.tab_3 = QtWidgets.QWidget()
        self.tab_3.setObjectName("tab_3")
        self.verticalLayout_4 = QtWidgets.QVBoxLayout(self.tab_3)
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.graphicsView_7d = PlotWidget(self.tab_3)
        self.graphicsView_7d.setObjectName("graphicsView_7d")
        self.verticalLayout_4.addWidget(self.graphicsView_7d)
        self.tabWidget.addTab(self.tab_3, "")
        self.tab_4 = QtWidgets.QWidget()
        self.tab_4.setObjectName("tab_4")
        self.verticalLayout_5 = QtWidgets.QVBoxLayout(self.tab_4)
        self.verticalLayout_5.setObjectName("verticalLayout_5")
        self.graphicsView_30d = PlotWidget(self.tab_4)
        self.graphicsView_30d.setObjectName("graphicsView_30d")
        self.verticalLayout_5.addWidget(self.graphicsView_30d)
        self.tabWidget.addTab(self.tab_4, "")
        self.tab_5 = QtWidgets.QWidget()
        self.tab_5.setObjectName("tab_5")
        self.verticalLayout_6 = QtWidgets.QVBoxLayout(self.tab_5)
        self.verticalLayout_6.setObjectName("verticalLayout_6")
        self.graphicsView_1y = PlotWidget(self.tab_5)
        self.graphicsView_1y.setObjectName("graphicsView_1y")
        self.verticalLayout_6.addWidget(self.graphicsView_1y)
        self.tabWidget.addTab(self.tab_5, "")
        self.tab_2 = QtWidgets.QWidget()
        self.tab_2.setObjectName("tab_2")
        self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.tab_2)
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        self.graphicsView_at = PlotWidget(self.tab_2)
        self.graphicsView_at.setObjectName("graphicsView_at")
        self.verticalLayout_3.addWidget(self.graphicsView_at)
        self.tabWidget.addTab(self.tab_2, "")
        self.verticalLayout.addWidget(self.tabWidget)
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 800, 29))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        self.tabWidget.setCurrentIndex(3)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.label.setText(_translate("MainWindow", "SOL Price"))
        self.label_price.setText(_translate("MainWindow", "---"))
        self.label_3.setText(_translate("MainWindow", "Change"))
        self.label_change.setText(_translate("MainWindow", "---"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), _translate("MainWindow", "24 Hours"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_3), _translate("MainWindow", "7 Days"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_4), _translate("MainWindow", "30 Days"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_5), _translate("MainWindow", "1 Year"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), _translate("MainWindow", "All-Time"))
from pyqtgraph import PlotWidget


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec())
