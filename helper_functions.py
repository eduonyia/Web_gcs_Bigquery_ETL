# Time Zone helper function
def time_of_day(x):
    if x in range(6, 12):
        return "Morning"
    elif x in range(12, 16):
        return "Afternoon"
    elif x in range(16, 22):
        return "Evening"
    else:
        return "Late night"


def day_of_week(x):
    if x == 0:
        return "Monday"
    elif x == 1:
        return "Tuesday"
    elif x == 2:
        return "Wednesday"
    elif x == 3:
        return "Thursday"
    elif x == 4:
        return "Friday"
    elif x == 5:
        return "Saturday"
    elif x == 6:
        return "Sunday"
    else:
        return "Invalid Day"
