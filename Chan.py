import copy
import datetime
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Union

from BuySellPoint.BS_Point import CBS_Point
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE
from Common.ChanException import CChanException, ErrCode
from Common.CTime import CTime
from Common.func_util import check_kltype_order, kltype_lte_day
from DataAPI.CommonStockAPI import CCommonStockApi
from KLine.KLine_List import CKLine_List
from KLine.KLine_Unit import CKLine_Unit


class CChan:
    def __init__(
        self,
        code,
        begin_time=None,
        end_time=None,
        data_src: Union[DATA_SRC, str] = "custom:MySQL_API.CMySQLAPI",
        lv_list=None,
        config=None,
        autype: AUTYPE = AUTYPE.QFQ,
    ):
        if lv_list is None:
            lv_list = [KL_TYPE.K_DAY, KL_TYPE.K_60M]
        check_kltype_order(lv_list)  # lv_list顺序从高到低
        self.code = code
        self.begin_time = str(begin_time) if isinstance(begin_time, datetime.date) else begin_time
        self.end_time = str(end_time) if isinstance(end_time, datetime.date) else end_time
        self.autype = autype
        self.data_src = data_src
        self.lv_list: List[KL_TYPE] = lv_list

        if config is None:
            config = CChanConfig()
        self.conf = config

        self.kl_misalign_cnt = 0
        self.kl_inconsistent_detail = defaultdict(list)

        self.g_kl_iter = defaultdict(list)

        self.do_init()

        if not config.trigger_step:
            for _ in self.load():
                ...

    def __deepcopy__(self, memo):
        cls = self.__class__
        obj: CChan = cls.__new__(cls)
        memo[id(self)] = obj
        obj.code = self.code
        obj.begin_time = self.begin_time
        obj.end_time = self.end_time
        obj.autype = self.autype
        obj.data_src = self.data_src
        obj.lv_list = copy.deepcopy(self.lv_list, memo)
        obj.conf = copy.deepcopy(self.conf, memo)
        obj.kl_misalign_cnt = self.kl_misalign_cnt
        obj.kl_inconsistent_detail = copy.deepcopy(self.kl_inconsistent_detail, memo)
        obj.g_kl_iter = copy.deepcopy(self.g_kl_iter, memo)
        if hasattr(self, 'klu_cache'):
            obj.klu_cache = copy.deepcopy(self.klu_cache, memo)
        if hasattr(self, 'klu_last_t'):
            obj.klu_last_t = copy.deepcopy(self.klu_last_t, memo)
        obj.kl_datas = {}
        for kl_type, ckline in self.kl_datas.items():
            obj.kl_datas[kl_type] = copy.deepcopy(ckline, memo)
        for kl_type, ckline in self.kl_datas.items():
            for klc in ckline:
                for klu in klc.lst:
                    assert id(klu) in memo
                    if klu.sup_kl:
                        memo[id(klu)].sup_kl = memo[id(klu.sup_kl)]
                    memo[id(klu)].sub_kl_list = [memo[id(sub_kl)] for sub_kl in klu.sub_kl_list]
        return obj

    def do_init(self):
        self.kl_datas: Dict[KL_TYPE, CKLine_List] = {}
        for idx in range(len(self.lv_list)):
            self.kl_datas[self.lv_list[idx]] = CKLine_List(self.lv_list[idx], conf=self.conf)

    def load_stock_data(self, stockapi_instance: CCommonStockApi, lv) -> Iterable[CKLine_Unit]:
        for KLU_IDX, klu in enumerate(stockapi_instance.get_kl_data()):
            klu.set_idx(KLU_IDX)
            klu.kl_type = lv
            yield klu

    def get_load_stock_iter(self, stockapi_cls, lv):
        stockapi_instance = stockapi_cls(code=self.code, k_type=lv, begin_date=self.begin_time, end_date=self.end_time, autype=self.autype)
        return self.load_stock_data(stockapi_instance, lv)

    def add_lv_iter(self, lv_idx, iter):
        if isinstance(lv_idx, int):
            self.g_kl_iter[self.lv_list[lv_idx]].append(iter)
        else:
            self.g_kl_iter[lv_idx].append(iter)

    def get_next_lv_klu(self, lv_idx):
        if isinstance(lv_idx, int):
            lv_idx = self.lv_list[lv_idx]
        if len(self.g_kl_iter[lv_idx]) == 0:
            raise StopIteration
        try:
            return self.g_kl_iter[lv_idx][0].__next__()
        except StopIteration:
            self.g_kl_iter[lv_idx] = self.g_kl_iter[lv_idx][1:]
            if len(self.g_kl_iter[lv_idx]) != 0:
                return self.get_next_lv_klu(lv_idx)
            else:
                raise

    def step_load(self):
        assert self.conf.trigger_step
        self.do_init()  # 清空数据，防止再次重运没有数据
        yielded = False  # 是否曾经返回过结果
        for idx, snapshot in enumerate(self.load(self.conf.trigger_step)):
            if idx < self.conf.skip_step:
                continue
            yield snapshot
            yielded = True
        if not yielded:
            yield self

    def trigger_load(self, inp):
        # {type: [klu, ...]}
        if not hasattr(self, 'klu_cache'):
            self.klu_cache: List[Optional[CKLine_Unit]] = [None for _ in self.lv_list]
        if not hasattr(self, 'klu_last_t'):
            self.klu_last_t = [CTime(1980, 1, 1, 0, 0) for _ in self.lv_list]
        for lv_idx, lv in enumerate(self.lv_list):
            if lv not in inp:
                if lv_idx == 0:
                    raise CChanException(f"最高级别{lv}没有传入数据", ErrCode.NO_DATA)
                continue
            for klu in inp[lv]:
                klu.kl_type = lv
            assert isinstance(inp[lv], list)
            self.add_lv_iter(lv, iter(inp[lv]))
        for _ in self.load_iterator(lv_idx=0, parent_klu=None, step=False):
            ...
        if not self.conf.trigger_step:  # 非回放模式全部算完之后才算一次中枢和线段
            for lv in self.lv_list:
                self.kl_datas[lv].cal_seg_and_zs()

    def init_lv_klu_iter(self, stockapi_cls):
        # 为了跳过一些获取数据失败的级别
        lv_klu_iter = []
        valid_lv_list = []
        for lv in self.lv_list:
            try:
                lv_klu_iter.append(self.get_load_stock_iter(stockapi_cls, lv))
                valid_lv_list.append(lv)
            except CChanException as e:
                if e.errcode == ErrCode.SRC_DATA_NOT_FOUND and self.conf.auto_skip_illegal_sub_lv:
                    if self.conf.print_warning:
                        print(f"[WARNING-{self.code}]{lv}级别获取数据失败，跳过")
                    del self.kl_datas[lv]
                    continue
                raise e
        self.lv_list = valid_lv_list
        return lv_klu_iter

    def GetStockAPI(self):
        if isinstance(self.data_src, str) and self.data_src.startswith("custom:"):
            # Custom data source, format: "custom:module_path:ClassName"
            _, module_path, class_name = self.data_src.split(":")
            # Import the module dynamically
            try:
                module = __import__(module_path, fromlist=[class_name])
                stockapi_cls = getattr(module, class_name)
                return stockapi_cls
            except ImportError as e:
                raise CChanException(f"Module import error: {e}", ErrCode.PARA_ERROR)
            except AttributeError as e:
                raise CChanException(f"Class not found: {e}", ErrCode.PARA_ERROR)
        else:
            # Existing code for predefined data sources
            data_cls = DATA_SRC2CLS.get(self.data_src)
            if not data_cls:
                raise CChanException(f"Data source {self.data_src} not recognized", ErrCode.PARA_ERROR)
            return data_cls

    def load(self, step=False):
        stockapi_cls = self.GetStockAPI()
        try:
            stockapi_cls.do_init()
            for lv_idx, klu_iter in enumerate(self.init_lv_klu_iter(stockapi_cls)):
                self.add_lv_iter(lv_idx, klu_iter)
            self.klu_cache: List[Optional[CKLine_Unit]] = [None for _ in self.lv_list]
            self.klu_last_t = [CTime(1980, 1, 1, 0, 0) for _ in self.lv_list]

            yield from self.load_iterator(lv_idx=0, parent_klu=None, step=step)  # 计算入口
            if not step:  # 非回放模式全部算完之后才算一次中枢和线段
                for lv in self.lv_list:
                    self.kl_datas[lv].cal_seg_and_zs()
        except Exception:
            if self.data_src == "custom:MySQL_API.CMySQLAPI":
                # If MySQL fails, fallback to BaoStock for incremental real-time data
                self.data_src = DATA_SRC.BAO_STOCK
                return self.load(step=step)
            raise
        finally:
            stockapi_cls.do_close()
        if len(self[0]) == 0:
            raise CChanException("\u6700\u9ad8\u7ea7\u522b\u6ca1\u6709\u83b7\u5f97\u4efb\u4f55\u6570\u636e", ErrCode.NO_DATA)

    def set_klu_parent_relation(self, parent_klu, kline_unit, cur_lv, lv_idx):
        if self.conf.kl_data_check and kltype_lte_day(cur_lv) and kltype_lte_day(self.lv_list[lv_idx-1]):
            self.check_kl_consitent(parent_klu, kline_unit)
        parent_klu.add_children(kline_unit)
        kline_unit.set_parent(parent_klu)

    def add_new_kl(self, cur_lv: KL_TYPE, kline_unit):
        try:
            self.kl_datas[cur_lv].add_single_klu(kline_unit)
        except Exception:
            if self.conf.print_err_time:
                print(f"[ERROR-{self.code}]\u5728\u8ba1\u7b97{kline_unit.time}K\u7ebf\u65f6\u53d1\u751f\u9519\u8bef!")
            raise

    def try_set_klu_idx(self, lv_idx: int, kline_unit: CKLine_Unit):
        if kline_unit.idx >= 0:
            return
        if len(self[lv_idx]) == 0:
            kline_unit.set_idx(0)
        else:
            kline_unit.set_idx(self[lv_idx][-1][-1].idx + 1)

    def load_iterator(self, lv_idx, parent_klu, step):
        # K\u7ebf\u65f6\u95f4\u5929\u7ea7\u522b\u4ee5\u4e0b\u63cf\u8ff0\u7684\u662f\u7ed3\u675f\u65f6\u95f4\uff0c\u598260M\u7ebf\uff0c\u6bcf\u5929\u7b2c\u4e00\u6839\u662f10\u70b930\u7684
        # \u5929\u4ee5\u4e0a\u662f\u5f53\u5929\u65e5\u671f
        cur_lv = self.lv_list[lv_idx]
        pre_klu = self[lv_idx][-1][-1] if len(self[lv_idx]) > 0 and len(self[lv_idx][-1]) > 0 else None
        while True:
            if self.klu_cache[lv_idx]:
                kline_unit = self.klu_cache[lv_idx]
                assert kline_unit is not None
                self.klu_cache[lv_idx] = None
            else:
                try:
                    kline_unit = self.get_next_lv_klu(lv_idx)
                    self.try_set_klu_idx(lv_idx, kline_unit)
                    if not kline_unit.time > self.klu_last_t[lv_idx]:
                        raise CChanException(f"kline time err, cur={kline_unit.time}, last={self.klu_last_t[lv_idx]}", ErrCode.KL_NOT_MONOTONOUS)
                    self.klu_last_t[lv_idx] = kline_unit.time
                except StopIteration:
                    break

            if parent_klu and kline_unit.time > parent_klu.time:
                self.klu_cache[lv_idx] = kline_unit
                break
            kline_unit.set_pre_klu(pre_klu)
            pre_klu = kline_unit
            self.add_new_kl(cur_lv, kline_unit)
            if parent_klu:
                self.set_klu_parent_relation(parent_klu, kline_unit, cur_lv, lv_idx)
            if lv_idx != len(self.lv_list)-1:
                for _ in self.load_iterator(lv_idx+1, kline_unit, step):
                    ...
                self.check_kl_align(kline_unit, lv_idx)
            if lv_idx == 0 and step:
                yield self

    def check_kl_consitent(self, parent_klu, sub_klu):
        if parent_klu.time.year != sub_klu.time.year or \
           parent_klu.time.month != sub_klu.time.month or \
           parent_klu.time.day != sub_klu.time.day:
            self.kl_inconsistent_detail[str(parent_klu.time)].append(sub_klu.time)
            if self.conf.print_warning:
                print(f"[WARNING-{self.code}]\u7236\u7ea7\u522b\u65f6\u95f4\u662f{parent_klu.time}\uff0c\u6b21\u7ea7\u522b\u65f6\u95f4\u5374\u662f{sub_klu.time}")
            if len(self.kl_inconsistent_detail) >= self.conf.max_kl_inconsistent_cnt:
                raise CChanException(f"\u7236&\u5b50\u7ea7\u522bK\u7ebf\u65f6\u95f4\u4e0d\u4e00\u81f4\u6761\u6570\u8d85\u8fc7{self.conf.max_kl_inconsistent_cnt}\uff01\uff01", ErrCode.KL_TIME_INCONSISTENT)

    def check_kl_align(self, kline_unit, lv_idx):
        if self.conf.kl_data_check and len(kline_unit.sub_kl_list) == 0:
            self.kl_misalign_cnt += 1
            if self.conf.print_warning:
                print(f"[WARNING-{self.code}]\u5f53\u524d{kline_unit.time}\u6ca1\u5728\u6b21\u7ea7\u522b{self.lv_list[lv_idx+1]}\u627e\u5230K\u7ebf\uff01\uff01")
            if self.kl_misalign_cnt >= self.conf.max_kl_misalgin_cnt:
                raise CChanException(f"\u5728\u6b21\u7ea7\u522b\u627e\u4e0d\u5230K\u7ebf\u6761\u6570\u8d85\u8fc7{self.conf.max_kl_misalgin_cnt}\uff01\uff01", ErrCode.KL_DATA_NOT_ALIGN)

    def __getitem__(self, n) -> CKLine_List:
        if isinstance(n, KL_TYPE):
            return self.kl_datas[n]
        elif isinstance(n, int):
            return self.kl_datas[self.lv_list[n]]
        else:
            raise CChanException("unspoourt query type", ErrCode.COMMON_ERROR)

    def get_bsp(self, idx=None) -> List[CBS_Point]:
        if idx is not None:
            return sorted(self[idx].bs_point_lst.lst, key=lambda x: x.klu.time)
        assert len(self.lv_list) == 1
        return sorted(self[0].bs_point_lst.lst, key=lambda x: x.klu.time)


