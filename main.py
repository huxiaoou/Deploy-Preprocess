import argparse


def parse_args():
    arg_parser = argparse.ArgumentParser(description="This project is designed to create preprocess data by instrument")
    arg_parser.add_argument("command", type=str, choices=("preprocess", "dominant", "avlb", "icov", "market", "css"))
    arg_parser.add_argument("--bgn", type=str, required=True, help="begin date, format = 'YYYYMMDD'")
    arg_parser.add_argument("--end", type=str, default=None, help="end date, format = 'YYYYMMDD'")
    return arg_parser.parse_args()


if __name__ == "__main__":
    import sys
    from qtools_sxzq.qcalendar import CCalendar
    from config import cfg, data_desc_preprocess

    args = parse_args()
    bgn = args.bgn
    end = args.end or bgn
    span: tuple[str, str] = (args.bgn, args.end)
    calendar = CCalendar(calendar_path=cfg.path_calendar)
    if not calendar.is_trade_date(bgn) or not calendar.is_trade_date(end):
        print(f"[INF] {bgn} or {end} is not in trade calendar, please check again")
        sys.exit()

    if args.command == "preprocess":
        from solutions.preprocess import main_preprocess
        from config import data_desc_pv, data_desc_funda

        slc_vars = [
            "open",
            "high",
            "low",
            "close",
            "settle",
            "volume",
            "amt",
            "open_interest",
            "multiplier",
        ]

        main_preprocess(
            codes=cfg.codes,
            bgn=bgn,
            end=end,
            cfg_major=cfg.major,
            data_desc_pv=data_desc_pv,
            data_desc_funda=data_desc_funda,
            data_desc_preprocess=data_desc_preprocess,
            slc_vars=slc_vars,
            calendar=calendar,
        )
    elif args.command == "dominant":
        from solutions.dominant import main_dominant
        from config import data_desc_dominant

        main_dominant(
            bgn=bgn,
            end=end,
            data_desc_preprocess=data_desc_preprocess,
            data_desc_dominant=data_desc_dominant,
            calendar=calendar,
        )
    elif args.command == "avlb":
        from solutions.avlb import main_process_avlb
        from config import data_desc_avlb

        data_desc_preprocess.lag = cfg.avlb.lag
        main_process_avlb(
            span=span,
            cfg_avlb=cfg.avlb,
            data_desc_pv=data_desc_preprocess,
            data_desc_avlb=data_desc_avlb,
        )
