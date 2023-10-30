from dateutil.relativedelta import TH
from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    Holiday,
    USColumbusDay,
    USLaborDay,
    USMartinLutherKingJr,
    USMemorialDay,
    USPresidentsDay,
    USThanksgivingDay,
    nearest_workday,
)
from pandas.tseries.offsets import DateOffset, Day


class CureatrHolidayCalendar(AbstractHolidayCalendar):
    """
    US Federal Government Holiday Calendar based on rules specified by:
    https://www.opm.gov/policy-data-oversight/
       snow-dismissal-procedures/federal-holidays/
    """

    rules = [
        Holiday("New Years Day", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        USMemorialDay,
        Holiday("Juneteenth", start_date="2021-06-17", month=6, day=19, observance=nearest_workday),
        Holiday("July 4th", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USColumbusDay,
        USThanksgivingDay,
        Holiday("Thanksgiving Friday", month=11, day=1, offset=[DateOffset(weekday=TH(4)), Day(1)]),
        Holiday("Christmas", month=12, day=25, observance=nearest_workday),
    ]


cal = CureatrHolidayCalendar()
holidays = cal.holidays(start="2010-01-01", end="2029-12-31", return_name=True)
holidays = holidays.reset_index(name="holiday").rename(columns={"index": "date_key"})
holidays.to_csv("seeds/stg_cureatr__holiday_calendar.csv", index=False)
