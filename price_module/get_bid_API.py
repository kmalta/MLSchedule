import urllib
import sys
import ast

region = 'us-west-1'


def get_cost(num_machines, inst_type):
    ret_dict = get_bid(inst_type, region)
    return ret_dict['bid']*num_machines


def get_bid(inst_type, region):
    url = 'http://128.111.84.183/' + region + '-' + inst_type + '.html'
    f = urllib.urlopen(url)
    data = f.readlines()
    script_index = -1
    for i, line in enumerate(data):
        if '<script>' in line:
            script_index = i
            break

    time_line = data[script_index + 2]
    cost_line = data[script_index + 3]


    times = ast.literal_eval('[' + time_line.split('[')[1].split(']')[0] + ']')
    costs = ast.literal_eval('[' + cost_line.split('[')[1].split(']')[0] + ']')

    first_hour_cliff_index = -1
    for i, elem in enumerate(times):
        if int(elem) >= 1:
            first_hour_cliff_index = i
            break

    time = float(times[i])
    price = float(costs[i])

    ret_dict = {}
    ret_dict['inst_type'] = inst_type
    ret_dict['region'] = region
    ret_dict['bid'] = price
    ret_dict['duration'] = time
    return ret_dict











def design_experiments(budget, duration, training_budget, duration_budget, inst_types):

    inst_type_experiments = []
    for exp_count, inst_type in enumerate(inst_types):
        scales = [2**i for i in range(1,11)]
        machs = []
        i = 1
        while get_cost(2**i, inst_type) <= budget*training_budget:
            machs.append(i)
            i += 1

        if machs == []:
            print 'The instance type selected was too expensive to run an experiment on our budget.  Skipping.'
            continue

        exps = []
        for i in range(len(scales)):
            for j in range(len(machs)):
                exps.append([1, scales[i], machs[j], 2**machs[j]])
        print '\nExperiment', str(exp_count) + ':\t' + inst_type + '\n'
        optimal_experiments = A_optimal_experimental_design(exps, training_budget*budget, duration_budget*duration)
        for i in range(len(optimal_experiments)):
            optimal_experiments[i] = optimal_experiments[i][1:-1]
            optimal_experiments[i][0] = float(100)/optimal_experiments[i][0]
        inst_type_experiments.append(optimal_experiments)
    return inst_type_experiments










def A_optimal_experimental_design(exps, B_init, T_init):

    num_exps = len(exps)

    # Turn experiments list into a cvx matrix
    A = cvx.matrix(exps)

    # print experiments list

    # Create new problem
    prob =pic.Problem()

    A = pic.new_param('A', A)

    lamb = prob.add_variable('lamb',num_exps, lower=0)
    targ = prob.add_variable('targ',1, lower=0)


    # Bounds lambda between 0 and 1 for each var in lambda
    #prob.add_list_of_constraints( [abs(lamb[i]) <= 1 for i in range(num_exps)], 'i', '[num_exps]')
    prob.add_constraint(1 | lamb < 1)

    # Creates monitary and duration constraints on initialization problem
    #prob.add_constraint(pic.sum([lamb[i]*exps[i][0] for i in range(num_exps)], 'i') <= T_init)

    # Creates covariance matrix
    cov_mat = pic.sum([lamb[i]*A[i]*A[i].T for i in range(num_exps)],'i', '[num_exps]')

    prob.add_constraint(targ > pic.tracepow(cov_mat,-1))
    prob.set_objective('min',targ)


    #print prob
    #print 'type:   ' + prob.type
    #print 'status: ' + prob.status
    prob.solve(verbose=0)
    #print 'status: ' + prob.status

    lamb=lamb.value
    w=lamb/sum(lamb) #normalize mu to get the optimal weights
    #print
    #print 'The optimal design is:'
    #print repr(list(w))
    filtered_exps = filter(lambda x: x[1] > 1e-3, enumerate(w))
    ret_exps = []
    for idx in filtered_exps:
        print repr(exps[idx[0]]), str(float(idx[1])*100) + '%'
        ret_exps.append(exps[idx[0]])
    return ret_exps














def main():
    clp = sys.argv
    inst_type = clp[1]
    region = clp[2]
    get_bid(inst_type, region)

if __name__ == "__main__":
    main()

