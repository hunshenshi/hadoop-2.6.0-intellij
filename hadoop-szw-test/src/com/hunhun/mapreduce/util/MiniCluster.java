package com.hunhun.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;

import java.io.IOException;

/**
 * Created by hunhun on 2017/4/7.
 */
public class MiniCluster {

    private MiniMRClientCluster mrCluster;

    private class InternalClass {
    }

    private static class InnerMiniCluster{
        private static MiniCluster miniCluster = new MiniCluster();
    }

    public static MiniCluster getMiniCluster(){
        return InnerMiniCluster.miniCluster;
    }

    public MiniMRClientCluster setup() {
        // create the mini cluster to be used for the tests
        try {
            mrCluster = MiniMRClientClusterFactory.create(InternalClass.class, 1,
                    new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mrCluster;
    }

    public void cleanup() {
        // stopping the mini cluster
        try {
            mrCluster.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private MiniCluster(){
//        try {
//            miniMRClientCluster = MiniMRClientClusterFactory.create(InternalClass.class, 1,
//                    new Configuration());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
