package scheduler

import (
	"encoding/json"
	"fmt"

	"github.com/ping-42/42lib/db/models"
	"github.com/ping-42/42lib/dns"
	"github.com/ping-42/42lib/http"
	"github.com/ping-42/42lib/icmp"
	"github.com/ping-42/42lib/traceroute"
)

func factoryTaskMessage(t models.Task) (res []byte, err error) {

	switch t.TaskTypeID {
	case models.TASK_DNS:
		testDnsTask, er := dns.NewTaskFromModel(t)
		if er != nil {
			err = er
			return
		}

		res, err = json.Marshal(testDnsTask) //nolint
		if err != nil {
			err = fmt.Errorf("json.Marshal(testDnsTask), %v", err)
			return
		}

	case models.TASK_ICMP:
		testIcmpTask, er := icmp.NewTaskFromModel(t)
		if er != nil {
			err = er
			return
		}

		res, err = json.Marshal(testIcmpTask)
		if err != nil {
			err = fmt.Errorf("json.Marshal(testIcmpTask), %v", err)
			return
		}

	case models.TASK_HTTP:
		testHttpTask, er := http.NewTaskFromModel(t)
		if er != nil {
			err = er
			return
		}

		res, err = json.Marshal(testHttpTask)
		if err != nil {
			err = fmt.Errorf("json.Marshal(testHttpTask), %v", err)
			return
		}

	case models.TASK_TRACEROUTE:
		testTracerouteTask, er := traceroute.NewTaskFromModel(t)
		if er != nil {
			err = er
			return
		}

		res, err = json.Marshal(testTracerouteTask)
		if err != nil {
			err = fmt.Errorf("json.Marshal(testTracerouteTask), %v", err)
			return
		}

	default:
		err = fmt.Errorf("unxpected TaskTypeID:%v", t.TaskTypeID)
		return
	}

	return
}

// layer between subscription.Opts and task.Opts
func factoryTaskOpts(clientSubscription models.Subscription) (res []byte, err error) {
	return clientSubscription.Opts, nil
}
