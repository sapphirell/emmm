<?php

class Model_Hz_DataStatistics extends Model_Base
{
    protected $db;
    protected $redis;
    protected $table;
    protected $cacheKey;

    /**
     * Model_Hz_DataStatistics constructor.
     */
    public function __construct()
    {
        $this->db = Dao::getInstance();
    }

    /**
     * 概览数据、聚合数据导出
     * @param array $time 时间 ['time_start' => $time['time_start'], 'time_start' =>$time['time_end']]
     * @param int $type 分组类型
     * @return bool
     */
    public function hzOverview($time, $type=1, $brand)
    {
//        $dateMap = array('2017-11-01 00:00:00','2017-11-02 00:00:00');
        $date = date("Y-m-d", strtotime($time['time_start']));
        $dateMap = $this->getDateFromRange($time['time_start'], $time['time_end']);
        $key_arr = array(
            1 => 'date,version',
            2 => 'date',
            3 => 'date,version,brand',
            4 => 'date,version,brand,model'
        );
        $keys_type = str_replace(',', '_', $key_arr[$type]);
//        var_dump($keys_type);
        array_walk($dateMap, function ($date) use ($type, $time, $key_arr, $keys_type) {
            if (!$type || $type == 1)
            {
                $sql = "SELECT * FROM my_hz_data_statistics_middle WHERE `date` = '{$date}' AND `keys_type` = '{$keys_type}' ";
            }

            if ($type == '2')
            {
                $sql = "SELECT * FROM my_hz_data_statistics_middle WHERE `date` = '{$date}' AND `keys_type` = '{$keys_type}' ";
            }
            if ($type == '3')
            {
                $sql = "SELECT * FROM my_hz_data_statistics_middle WHERE `date` = '{$date}' AND `keys_type` = '{$keys_type}' ";
            }
            if ($type == '4')
            {
                $sql = "SELECT * FROM my_hz_data_statistics_middle WHERE `date` = '{$date}' AND `keys_type` = '{$keys_type}' ";
            }
//            echo $sql;

            $res = $this->db->createCommand($sql)->getAll();
//            var_dump(empty($res));
//            if (empty($res))
//            {
//                echo $sql;
//            }
            if (empty($res))
            {
                //如果没有聚合数据就先生成聚合数据
                $this->prepareDatas(array('time_start' => $date." 00:00:00", 'time_end' => $date." 23:59:59"), $type);
            }
        });

        $sql = "SELECT * FROM my_hz_data_statistics_middle WHERE `date` >= '{$time['time_start']}' AND `date` <= '{$time['time_end']}' ";
        $sql .= " AND `keys_type` = '{$keys_type}' " ;

        $res = $this->db->createCommand($sql)->getAll();
        //整理为
        foreach ($res as &$value)
        {
            $value['datas'] = unserialize($value['datas']);
        }
        return $res;
    }

    /**
     * 这些玩意查询太久了，因此事先存个缓存表。这个函数可以查、返回查的res、存到暂存表
     * @param array $date ['2017-11-01 00:00:00','2017-11-02 00:00:00']
     * @param string $group_type group 限定
     * @return array
     */
    public function prepareDatas(array $date, $group_type = '1')
    {
        if (empty($date))
        {
            return false;
        }
        $gtype      = array(
            1 => 'date,version',
            2 => 'date',
            3 => 'date,version,brand',
            4 => 'date,version,brand,model'
        );
        $group = $gtype[$group_type];
        $key_type = str_replace(',','_', $group);

        $time_start = $date['time_start'];
        $time_end   = $date['time_end'];

        $today      = date("Y-m-d", strtotime($time_start));

//        $sql = <<<EOF
//                select
//                hz_imei,version ,cdate,`time`,
//				DATE_FORMAT(`cdate`,'%Y%m%d') as date,
//                substring_index(substring_index(content,'totalTime\\\":',-1),',',1) as totalTime,/*总安装时长*/
//                substring_index(substring_index(content,'uid\\\":\\\"',-1),'\\\',1) as uid,
//                substring_index(substring_index(content,'mobileBrand\\\":\\\"',-1),'\\\',1) as mobileBrand,
//                substring_index(substring_index(content,'model\\\":\\\"',-1),'\\\',1) as model,
//                substring_index(substring_index(content,'installFail\\\":',-1),',',1) as installFail,
//                substring_index(substring_index(content,'installSuccess\\\":',-1),',',1) as installSuccess,
//                substring_index(substring_index(content,'installType\\\":',-1),'}',1) as installType,
//                substring_index(substring_index(content,'installTime\\\":',-1),',',1) as installTime,
//				substring_index(substring_index(content,'totalDuration\\\":',-1),',',1) as totalDuration,
//                substring_index(substring_index(content,'固件检测默认桌面是否成功：',-1),'.',1) as is_success
//                from my_hz_error_logs_201711
//                where cdate >= '{$time_start}' and cdate <= '{$time_end}' and module='LOG_COLLECTION' and file='AppInstall.cpp'
//                HAVING uid !=''and uid not REGEXP '0000000' AND totalTime > '100'
//EOF;
//
//        $sql = "SELECT *, count(*),
//                avg(case when totaltime>2000 then 2000 else totaltime end)/60 as totalTime ,
//                sum(case when is_success='success' then 1 else 0 end)/count(*) as successRate,  /*手机设置桌面默认成功率*/
//                sum(installTime) as installTime /*软件安装时间*/
//                FROM ($sql) as tmp
//                GROUP BY {$group}
//                ORDER BY date desc ";

        //基本的sql
//        $res_base = $this->db->createCommand($sql)->getAll();
        //LOG_COLLECTION想了想还是拆掉，降低报表耦合度
        $res_base = $this->collectionLog($date, $group_type);
//        var_dump($res_base);
        //优化时间和关闭成功率
        $res_v2 = $this->closeStatistics(array($time_start, $time_end), $group_type);
//        var_dump($res_v2);
        //激活数据
        $sql = "select * FROM `my_channel_info_collect` where cdate>='{$time_start}' and cdate<='{$time_end}' AND `type` = '3' order by cdate desc ";

        $res_dlist = $this->db->createCommand($sql)->getAll();

        //merge array
        foreach ($res_base as $key => &$value)
        {
            //合并优化时间
            foreach ($res_v2 as $item)
            {
                if ($group_type == 1) // 如果以版本号分组，则需要根据日期+版本号合并v2数据
                {
                    if ($item['date'] == $value['date'] && $item['version'] == $value['version'])
                    {
                        $value['optimizationTime']  = $item['optimizationTime'];//优化时长
                        $value['closeSuccess']      = $item['closeSuccess'];//关闭应用市场自更新成功率
                    }
                }
                if ($group_type == 2)
                {
                    if ($item['date'] == $value['date'])
                    {
                        $value['optimizationTime']  = $item['optimizationTime'];//优化时长
                        $value['closeSuccess']      = $item['closeSuccess'];//关闭应用市场自更新成功率
                    }
                }
                if ($group_type == 3)
                {
                    if ($item['date'] == $value['date'] && $item['version'] == $value['version'] && $item['brand'] == $value['brand'])
                    {
//                        var_dump('ok');die;
                        $value['optimizationTime']  = $item['optimizationTime'];//优化时长
                        $value['closeSuccess']      = $item['closeSuccess'];//关闭应用市场自更新成功率
//                        var_dump($value);die;
                    }
                }
                if ($group_type == 4)
                {
                    if ($item['date'] == $value['date'] && $item['version'] == $value['version'] && $item['brand'] == $value['brand'] && $item['model'] == $value['model'] )
                    {
                        $value['optimizationTime']  = $item['optimizationTime'];//优化时长
                        $value['closeSuccess']      = $item['closeSuccess'];//关闭应用市场自更新成功率
                    }
                }
            }

            //合并激活数据
            foreach ($res_dlist as $item)
            {
                if ($item['cdate'] == $value['date'])
                {
                    //单台激活软件数=激活软件数/激活台数
                    $value['single_active'] = $item['active_num_normal'] / $item['imei_active_normal'];
                    //单台软件安装数
                    $value['single_install'] = $item['install_soft_unique'] / $item['unique_install_imei'];
                    //激活率 = 重后的激活台数/安装台数
                    $value['proportion_active'] = $item['imei_active_normal'] / $item['unique_install_imei'];
                }
            }
            if ($group_type == '1' || $group_type == '3' || $group_type == '4')
            {
//                var_dump($value);die;
                //如果需要按版本分组的话，每天都要存各个版本一份的记录
                $datas = addslashes(serialize(($value)));
                $insert_sql = "INSERT INTO my_hz_data_statistics_middle SET `date` = '{$today}' , `keys` = '{$value['version']}' ,`keys_type`='{$key_type}', `datas` = '{$datas}'";
//                echo $insert_sql;die;
                $ins_res = $this->db->createCommand($insert_sql)->exec();
//                var_dump($ins_res);
//                if ($value['optimizationTime'])
//                {
//                    echo $insert_sql;
//                    var_dump($ins_res);die;
//                }

            }
        }
//        var_dump($res_base);
        if ($group_type == '2')
        {
            $datas = addslashes(serialize(gbk2utf8($res_base[0])));
            $this->db->createCommand("INSERT INTO my_hz_data_statistics_middle SET `date` = '{$today}' ,`keys_type`='{$key_type}', `datas` = '{$datas}'")->exec();
        }

        return $res_base;
    }

    /**
     * 关闭应用市场自更新成功率,生成一天的数据大概要1分钟
     * @param array$timeMap
     * @param string $group
     * @param string $brand
     * @return bool
     */
    public function closeStatistics($timeMap, $group = '1' ,$brand)
    {
        $time_start = $timeMap[0];//2017-11-10 00:00:00
        $time_end   = $timeMap[1];

        $dateMap    = $this->getDateFromRange($time_start, $time_end);

        $gtype      = array(
            4 => ' `date`, model',
            3 => ' `date`,Lower(`brand`)  ',
            2 => ' `date` ',
            1 => ' `date`,version '
        );

        $step_arr = array(
            'HUAWEI'    => array('停用', '停用应用', '确定', '必备应用', '卸载'),
            'Huawei'    => array('停用', '停用应用', '确定', '必备应用', '卸载'),
            'vivo'      => array('自更新提示', '应用更新提示', '消息推送', '自动更新', '首页快速评分', '桌面图标更新提醒'),
            'HONOR'     => array('停用', '确定', '必备应用', '卸载'),
            'xiaomi'    => array('更新提醒', '接受推送通知', '自动升级', '关于', '与新版保持同步', '必备应用', '卸载'),
            'Xiaomi'    => array('更新提醒', '接受推送通知', '自动升级', '关于', '与新版保持同步', '必备应用', '卸载'),
            'OPPO'      => array('WLAN自动更新','消息提醒','必备应用','卸载'),
            'GIONEE'    => array('自动更新', '接收推荐内容','必备应用', '停用')

        );
        //遍历生成日期段内的数据

        array_walk($dateMap, function ($value) use ($step_arr) {
            //2017-11-01
            $time_start = $value." 00:00:00";
            $time_end   = $value." 23:59:59";
            $get_v2 = "SELECT * FROM my_hz_data_statistics_v2 WHERE date = '$value'";
            $res = $this->db->createCommand($get_v2)->getAll();
            $Ym         = date("Ym", strtotime($value));
            if (empty($res))
            {
                $sql = <<<EOF
                   SELECT
                    SUBSTRING_INDEX(substring_index(content,':',-1), '}' ,'1') as optimizationTime,
                    content,version,REPLACE(LEFT(cdate,10),'-','') as cdate,
                    version,
                    SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX(content,'CLOSE_MARKET_UPDATE',1),'stepDuration\\\":',-1 ),',\\\"',1 ) as closeTime/*关闭时长*/
                    FROM
                        my_hz_error_logs_{$Ym}
                    WHERE
                        1
                    AND cdate >= '{$time_start}'
                    AND cdate <= '{$time_end}'
                    AND title = 'v2'
EOF;
                $res_v2 = $this->db->createCommand($sql)->getAll();

                $tmpArr = array();
                foreach ($res_v2 as $value)
                {
                    //以date+机型分组
                    $json_arr = json_decode(gbk2utf8(stripslashes($value['content'])), true);
                    //解析content字段
                    $res = $this->eachContent($json_arr, $step_arr);

                    //优化时长
                    $tmpArr[$value['cdate']][$res['brand']][$res['model']][$value['version']]['optimizationTime'] += $value['optimizationTime'];
                    //关闭时长
                    $tmpArr[$value['cdate']][$res['brand']][$res['model']][$value['version']]['stepDuration'] += $value['closeTime'];
                    if ($res['result'] == true)
                    {
                        $tmpArr[$value['cdate']][$res['brand']][$res['model']][$value['version']]['success'] += 1;
                    }
                    $tmpArr[$value['cdate']][$res['brand']][$res['model']][$value['version']]['count'] += 1;
                    if ($res['step'])
                    {
                        //false的步骤
                        $tmpArr[$value['cdate']][$res['brand']][$res['model']][$value['version']]['step'][$res['step']] += 1;
                    }
                }
                //对聚合数据进行入库
                if (!empty($tmpArr))
                {
                    $sql_ins = "INSERT INTO `my_hz_data_statistics_v2` (date,brand,model,version,fail_step,count_v2,success_v2,optimizationTime,stepDuration) VALUES ";
                    //数组样例 * 5维数组 *
                    /**
                     * [
                     *      日期 => [ 品牌=>[机型=>[盒子版本号=>[[时长，成功数，总数]]]]
                     * ]
                     */
                    foreach ($tmpArr as $date => $value)
                    {
                        array_walk($value, function ($b, $brand) use (&$sql_ins, $date) {
                            array_walk($b, function ($model_arr, $model) use (&$sql_ins, $date, $brand) {
                                array_walk($model_arr, function ($item, $version) use (&$sql_ins, $date, $brand, $model) {
                                    $fail_step = empty($item['step']) ? '-' : array_walk($item['step'], function ($num, $step) use (&$fail_step) {
                                        $fail_step .= $step.":".$num.";";
                                    });
                                    $sql_ins .= "('{$date}','{$brand}','{$model}','{$version}','{$fail_step}','{$item['count']}','{$item['success']}','{$item['optimizationTime']}','{$item['stepDuration']}') ,";
                                });
                            });
                        });
                    }
                }
                $sql_ins = rtrim($sql_ins, ',');
//              echo $sql_ins;
                $this->db->createCommand($sql_ins)->exec();
            }
        });
        //得到平均值
        $get_v2 = " SELECT 
                        date,version,FORMAT(SUM(optimizationTime) / SUM(count_v2),2) as optimizationTime,
                        FORMAT(SUM(success_v2) / SUM(count_v2) *100,2) as closeSuccess ,
                        FORMAT(SUM(stepDuration) / SUM(count_v2),2) as stepDuration,/*平均关闭时长*/
                        brand,model,SUM(count_v2) as count_v2,SUM(success_v2) as closeSuccessCount,SUM(count_v2) - SUM(success_v2) as closeFailCount
                    FROM my_hz_data_statistics_v2 
                    WHERE 
                        `date` >= '{$time_start}' AND  `date` <= '{$time_end}' ";
        $get_v2 .= $brand ? " and brand = '{$brand}' " : "";
        $get_v2 .= "GROUP BY {$gtype[$group]}";
//        echo $get_v2;
        $res = $this->db->createCommand($get_v2)->getAll();
        return $res;
    }

    /**
     * @param array $json_arr
     * @param $step
     * @return mixed
     */
    public function eachContent($json_arr, $step)
    {
        foreach ($json_arr['stepDatas'] as $tmpValue)
        {
            $return['brand'] = $json_arr['brand'];
            $return['model'] = $json_arr['model'];
            $return['result'] = true;
            if ($tmpValue['stepType'] == 'CLOSE_MARKET_UPDATE')
            {
                $return['stepDuration'] = $tmpValue['stepDuration'];
                foreach ($tmpValue['operateDatas'] as $item)
                {
                    if (in_array(utf8ToGbk($item['operateName']), $step[$json_arr['brand']]))
                    {
                        if ($item['operateResult'] !== true)
                        {
                            $return['result'] = false;
                            $return['step'] = utf8ToGbk($item['operateName']);
                            break;
                        }
                    }
                }

                break;//如果检查到了'CLOSE_MARKET_UPDATE'这一个元素就跳出循环了
            }
        }
        return $return;
    }

    /**
     * 各种设置桌面的xx率,比概览统计更加精确,prepareDatas方法中虽然存在类似的sql，但那个方法生成的是多个表来源的聚合数据
     * 和这个方法中是不同的，
     */
    public function getDesktopStatistics($time, $group = 1, $brand)
    {
        $groupBy = array(
            -1 => ' `date` ',
            1  => ' `date`,`brand` ',
            2  => ' `date`,`brand`,`model` '
        );
//        $dateM = $this->getDateFromRange($time['time_start'], $time['time_end']);

        array_walk($this->getDateFromRange($time['time_start'], $time['time_end']), function ($date) {
            //为空白的天存储暂存值
            $sql = "select * from `my_hz_data_statistics_desktop` where date = '{$date}'";
            $res = $this->db->createCommand($sql)->getRow();
            if (empty($res))
            {
                $this->setDesktopStatistics(array('time_start' => $date.' 00:00:00', 'time_end' => $date.' 23:59:59'));
            }
        });
        $sql = "select id , date , version , brand , model,
                sum(count) as count ,
                sum(successCount) as successCount,
                sum(failCount) as failCount,
                sum(desktopInstallSuccessCount) as desktopInstallSuccessCount,
                sum(triggerSetCount) as triggerSetCount,
                sum(totalTime) as totalTime,
                avg(successRate) as successRate,
                avg(setSuccessRate) as setSuccessRate,
                avg(phoneDesktopSetSuccessRate) as phoneDesktopSetSuccessRate,
                avg(avgTime) as avgTime
                from `my_hz_data_statistics_desktop` 
                where date >= '{$time['time_start']}' and date <= '{$time['time_end']}' ";
                $sql .= $brand ? " and brand = '{$brand}' " : '';
                $sql .= "GROUP by {$groupBy[$group]} ORDER BY date desc ,count desc";

        return $this->db->createCommand($sql)->getAll();
    }

    /**
     * 存储桌面xx率
     * @param array $time ['time_start' => '', 'time_end' => ''],时间跨度只能一天
     * @return string
     */
    public function setDesktopStatistics($time)
    {
//        $time_start = '2017-11-01 00:00:00';
//        $time_end = '2017-11-01 23:59:59';
        $time_start = $time['time_start'];
        $time_end   = $time['time_end'];
        $Ym         = date("Ym", strtotime($time['time_start']));
        $sql = <<<EOF
                select 
                hz_imei,version ,cdate,`time`,
				DATE_FORMAT(`cdate`,'%Y%m%d') as date,
                substring_index(substring_index(content,'totalTime\\\":',-1),',',1) as totalTime,/*总安装时长*/
                substring_index(substring_index(content,'uid\\\":\\\"',-1),'\\\',1) as uid,
                substring_index(substring_index(content,'mobileBrand\\\":\\\"',-1),'\\\',1) as mobileBrand,
                substring_index(substring_index(content,'model\\\":\\\"',-1),'\\\',1) as model,
                substring_index(substring_index(content,'installFail\\\":',-1),',',1) as installFail,
                substring_index(substring_index(content,'installSuccess\\\":',-1),',',1) as installSuccess,
                substring_index(substring_index(content,'installType\\\":',-1),'}',1) as installType,
                substring_index(substring_index(content,'installTime\\\":',-1),',',1) as installTime,
				substring_index(substring_index(content,'totalDuration\\\":',-1),',',1) as totalDuration,
                substring_index(substring_index(content,'固件检测默认桌面是否成功：',-1),'.',1) as is_success,
                substring_index(substring_index(content, '是否安装成功(' ,'-1') ,');',1) as isInstallSuccess
                from my_hz_error_logs_{$Ym}
                where cdate >= '{$time_start}' and cdate <= '{$time_end}' and module='LOG_COLLECTION' and file='AppInstall.cpp'
                HAVING uid !=''and uid not REGEXP '0000000' AND totalTime > '100' 
EOF;

        $sql = "SELECT 
                `date`,
                `version`,
                `mobileBrand` as brand,
                `model`,
                count(*) as `count`,
                sum(case when is_success='success' then 1 else 0 end) as successCount,/*设置桌面默认成功数*/
                sum(case when is_success='fail' then 1 else 0 end) as failCount,/*设置桌面默认失败数*/
                sum(case when isInstallSuccess='true' then 1 else 0 end) as desktopInstallSuccessCount,/*桌面安装成功数*/
                sum(case when is_success='success' then 1 else 0 end)+sum(case when is_success='fail' then 1 else 0 end) as triggerSetCount,/*触发设置桌面默认总数*/
                sum(case when totaltime>2000 then 2000 else totaltime end) as totalTime ,
                sum(case when is_success='success' then 1 else 0 end)/count(*) as successRate , /*设置桌面默认成功率*/
                sum(case when is_success='success' then 1 else 0 end)/(count(case when is_success='success' then 1 else 0 end)+count(case when is_success='fail' then 1 else 0 end)) as setSuccessRate,/*设置默认执行成功率=成功/成功+失败*/
                sum(case when is_success='success' then 1 else 0 end)/count(uid) as phoneDesktopSetSuccessRate,/*手机桌面设置默认成功率*/
                (sum(case when totaltime>2000 then 2000 else totaltime end)) / count(*) as avgTime /*设置桌面默认平均时长*/
                FROM ({$sql}) as tmp
                GROUP BY `date`,`mobileBrand`,`model`,`version`
                ";
//        echo $sql;die;
        $insert_sql = "INSERT INTO `my_hz_data_statistics_desktop` (`date`,`version`,`brand`,`model`,`count`,`successCount`,`failCount`,`desktopInstallSuccessCount`,`triggerSetCount`,`totalTime`,`successRate`,`setSuccessRate`,`phoneDesktopSetSuccessRate`,`avgTime`) ";
        $insert_sql .= $sql;
        $this->db->createCommand($insert_sql)->exec();

        return $insert_sql;
    }
    public function collectionLog($time,$group)
    {
        $gtype      = array(
            1  => ' `date`,`version`',
            2  => ' `date`  ',
            3  => ' `date`,`brand` ',
            4  => ' `date`,`brand`,`model` ',
            5  => ' `date`,`brand`,`model` ,`version`'
        );
        $group = $group?: 2 ;
        array_walk($this->getDateFromRange($time['time_start'], $time['time_end']), function ($date) use ($gtype) {
            $data_exists = "SELECT * FROM my_hz_data_statistics_collection WHERE date = '{$date}'";
            $ex = $this->db->createCommand($data_exists)->getRow();
            $time_start = $date . " 00:00:00";
            $time_end = $date . " 23:59:59";
            $Ym         = date("Ym", strtotime($date));
            $sql = <<<EOF
                select 
                hz_imei,version ,cdate,`time`,
				DATE_FORMAT(`cdate`,'%Y%m%d') as date,
                substring_index(substring_index(content,'totalTime\\\":',-1),',',1) as totalTime,/*总安装时长*/
                substring_index(substring_index(content,'uid\\\":\\\"',-1),'\\\',1) as uid,
                substring_index(substring_index(content,'mobileBrand\\\":\\\"',-1),'\\\',1) as mobileBrand,
                substring_index(substring_index(content,'model\\\":\\\"',-1),'\\\',1) as model,
                substring_index(substring_index(content,'installFail\\\":',-1),',',1) as installFail,
                substring_index(substring_index(content,'installSuccess\\\":',-1),',',1) as installSuccess,
                substring_index(substring_index(content,'installType\\\":',-1),'}',1) as installType,
                substring_index(substring_index(content,'installTime\\\":',-1),',',1) as installTime,
				substring_index(substring_index(content,'totalDuration\\\":',-1),',',1) as totalDuration,
                substring_index(substring_index(content,'固件检测默认桌面是否成功：',-1),'.',1) as is_success
                from my_hz_error_logs_{$Ym}
                where cdate >= '{$time_start}' and cdate <= '{$time_end}' and module='LOG_COLLECTION' and file='AppInstall.cpp'
                HAVING uid !=''and uid not REGEXP '0000000' AND totalTime > '100' 
EOF;

            $sql = "SELECT date,mobileBrand as brand,model,version, count(*) as `count_collection`,
                sum(case when is_success='success' then 1 else 0 end)/count(*) as successRate,  /*手机设置桌面默认成功率*/
                sum(case when installTime >2000 then 2000 else installTime end) as installTime, /*软件安装时间*/
                sum(case when totaltime>2000 then 2000 else totaltime end) as totalTime 
                FROM ($sql) as tmp
                GROUP BY {$gtype[5]} 
                ORDER BY date desc ";

            $sql = "INSERT INTO `my_hz_data_statistics_collection` (date,brand,model,version,count_collection,`successRate`,`installTime`,`totalTime`) "
                     . $sql;

            if (empty($ex))
            {
                //一天的数据16s
                $this->db->createCommand($sql)->exec();
            }
        });

        $data = "SELECT 
                       	date,
                        brand,
                        model,
                        version,
                        count_collection,
                        FORMAT(avg(successRate), 2) AS successRate,
                        SEC_TO_TIME(
                            FORMAT(
                                sum(installTime) / SUM(count_collection),
                                0
                            )AS installTime
                        ),
                        SEC_TO_TIME(
                            FORMAT(
                                SUM(totalTime) / SUM(count_collection),
                                0
                            ) AS totalTime
                        )
                 FROM my_hz_data_statistics_collection WHERE date >= '{$time['time_start']}' AND date <= '{$time['time_end']}'
                 GROUP by {$gtype[$group]}";
//        echo $data;DIE;
        //基本的sql
        $res_base = $this->db->createCommand($data)->getAll();
//        var_dump($res_base);die;
        return $res_base;
    }
    /**
     * 获取日期列表
     * @param string $start
     * @param string $end
     * @return array Y-m-d
     */
    public function getDateFromRange($start, $end)
    {
        $stimestamp = strtotime($start);
        $etimestamp = strtotime($end);

        // 计算日期段内有多少天
        $days = ($etimestamp - $stimestamp) / 86400 ;

        // 保存每天日期
        $date = array();
        for($i = 0; $i < $days; $i++)
        {
            $date[] = date('Y-m-d', $stimestamp + (86400 * $i));
        }
        return $date;
    }

    /**
     * @param string $remark 备注
     * @param int $id id
     * @return bool|mixed
     */
    public function saveRemark($remark, $id)
    {
        $sql = "UPDATE my_hz_data_statistics_middle SET remark = '{$remark}' WHERE id = '{$id}'";
        return $this->db->createCommand($sql)->exec();
    }
}