import argparse


def parse_args():
    arg_parser = argparse.ArgumentParser(description="This project is designed to create preprocess data by instrument")
    arg_parser.add_argument(
        "command", type=str, choices=("preprocess", "dominant", "avlb", "icov", "mkt", "css", "srets")
    )
    arg_parser.add_argument("--bgn", type=str, required=True, help="begin date, format = 'YYYYMMDD'")
    arg_parser.add_argument("--end", type=str, default=None, help="end date, format = 'YYYYMMDD'")
    return arg_parser.parse_args()


if __name__ == "__main__":
    import sys
    from qtools_sxzq.qcalendar import CCalendar
    from config import (
        cfg,
        data_desc_preprocess,
        data_desc_dominant,
        data_desc_avlb,
        data_desc_mkt,
        data_desc_icov,
        data_desc_css,
        data_desc_srets,
        data_desc_macro,
        data_desc_cpv,
        data_desc_funda,
    )

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
            data_desc_cpv=data_desc_cpv,
            data_desc_funda=data_desc_funda,
            data_desc_preprocess=data_desc_preprocess,
            slc_vars=slc_vars,
            calendar=calendar,
        )
    elif args.command == "dominant":
        from solutions.dominant import main_dominant

        main_dominant(
            bgn=bgn,
            end=end,
            data_desc_preprocess=data_desc_preprocess,
            data_desc_dominant=data_desc_dominant,
            calendar=calendar,
        )
    elif args.command == "avlb":
        from solutions.avlb import main_process_avlb

        data_desc_preprocess.lag = cfg.avlb.lag
        main_process_avlb(
            span=span,
            cfg_avlb=cfg.avlb,
            data_desc_pv=data_desc_preprocess,
            data_desc_avlb=data_desc_avlb,
        )
    elif args.command == "mkt":
        from solutions.mkt import main_process_mkt

        data_desc_preprocess.lag, data_desc_avlb.lag = 1, 1
        main_process_mkt(
            span=span,
            data_desc_pv=data_desc_preprocess,
            data_desc_avlb=data_desc_avlb,
            data_desc_macro=data_desc_macro,
            data_desc_mkt=data_desc_mkt,
        )
    elif args.command == "icov":
        from solutions.icov import main_process_icov

        data_desc_preprocess.lag = 240
        main_process_icov(
            span=span,
            cfg_icov=cfg.icov,
            data_desc_pv=data_desc_preprocess,
            data_desc_icov=data_desc_icov,
        )
    elif args.command == "css":
        from solutions.css import main_process_css

        data_desc_preprocess.lag, data_desc_avlb.lag = 1, 1
        main_process_css(
            span=span,
            cfg_css=cfg.css,
            data_desc_pv=data_desc_preprocess,
            data_desc_avlb=data_desc_avlb,
            data_desc_css=data_desc_css,
        )
    elif args.command == "srets":
        from solutions.srets import main_process_srets

        data_desc_preprocess.lag, data_desc_avlb.lag = 1, 1
        main_process_srets(
            span=span,
            sectors=sectors,
            universe_sector=universe_sector,
            data_desc_pv=data_desc_preprocess,
            data_desc_avlb=data_desc_avlb,
            dst_db=cfg_dbs.user,
            table_srets=cfg_tables.srets,
            project_data_dir=cfg.project_data_dir,
        )
