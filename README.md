# storm_car_analysis
大数据智慧交通系统
## 概述
 > 本次项目会以上海地图为模板，在地图上设置虚拟的卡口监控道路数据，每个卡口会实时收集路过车辆的车牌信息和路过时间。然后根据这些数据来实时计算卡口的车流量，判断哪些路段比较拥挤。另外将使用这些数据来进行套牌车的预警。如果在一定的时间和范围内在两个不同的卡口发现两辆相同的牌照的车，即可判断为套牌车。这些数据将会在web页面上进行显示。
## 运行环境配置
* JDK8、tomcat、MapReduce集群
 > 注：detail-records.zip为测试数据，hccDemo.war为WebUI，进入webapps/hccDemo/WEB-INF/classes目录，修改app.conf文件中的配置项为自己的环境配置实际值，配置项详细描述见app.conf文件中的注释
