import pygtk
pygtk.require('2.0')
import gtk, pango
import time


class Cal:

    def calendar_date_to_string(self):
         year, month, day = self.calendar.get_date()
         mytime = time.mktime((year, month+1, day, 0, 0, 0, 0, 0, -1))
         return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(mytime))

    def calendar_day_selected_double_click(self, widget):
         buffer = self.calendar_date_to_string()
#         year, month, day = self.calendar.get_date()
         self.text_field.set_text(buffer)
         self.window.destroy()
   

    def __init__(self,text_field):
        self.text_field = text_field
        self.calendar = None

        window = gtk.Window(gtk.WINDOW_TOPLEVEL)
        self.window = window
        window.set_title("Calendar Example")
        window.set_resizable(False)

        vbox = gtk.VBox(False)
        window.add(vbox)

        # The top part of the window, Calendar, flags and fontsel.
        hbox = gtk.HBox(False)
        vbox.pack_start(hbox, True, True)
        hbbox = gtk.HButtonBox()
        hbox.pack_start(hbbox, False, False)
        hbbox.set_layout(gtk.BUTTONBOX_SPREAD)
        hbbox.set_spacing(5)

        # Calendar widget
        frame = gtk.Frame("Calendar")
        hbbox.pack_start(frame, False, True)
        calendar = gtk.Calendar()
        calendar.connect('day_selected_double_click', self.calendar_day_selected_double_click)
        self.calendar = calendar
        frame.add(calendar)

        separator = gtk.VSeparator()
        hbox.pack_start(separator, False, True, 0)

        window.show_all()

