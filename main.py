import argparse


def parse_args():
    arg_parser = argparse.ArgumentParser(description="This project is designed to create preprocess data by instrument")
    arg_parser.add_argument("--bgn", type=str, required=True, help="begin date, format = 'YYYYMMDD'")
    arg_parser.add_argument("--end", type=str, default=None, help="end date, format = 'YYYYMMDD'")
    return arg_parser.parse_args()


if __name__ == "__main__":
    from qtools_sxzq.qcalendar import CCalendar
    from solutions.preprocess import main_preprocess
    from config import cfg
    from config import data_desc_pv

    args = parse_args()
    bgn = args.bgn
    end = args.end or bgn
    calendar = CCalendar(calendar_path=cfg.path_calendar)

    main_preprocess(
        codes=cfg.codes,
        bgn=bgn,
        end=end,
        calendar=calendar,
        data_desc_pv=data_desc_pv,
    )
