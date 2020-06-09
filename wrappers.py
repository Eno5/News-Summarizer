from contextlib import contextmanager as _contextmanager
from functools import wraps as _wraps
from concurrent.futures import ThreadPoolExecutor as _PoolExecutor
from itertools import tee as _tee
from functools import partial as _partial
from os import cpu_count as _cpu_count
from time import sleep as _sleep

def _deco(decorator):
    """Decorator for building decorators."""
    def wrapped_decorator(*args, **kwargs):
        if len(args) == 1 and callable(args[0]):
            return decorator(args[0], **kwargs)
        else:
            def real_decorator(decoratee):
                return decorator(decoratee, *args, **kwargs)
            return real_decorator
    return wrapped_decorator

@_deco
def retry(func=None, n=3, delay=0.25):
    """Decorator for retrying functions. If an error is raised, try again n times.
    Raises RuntimeError if there are no successful executions.
    
    
    Usage:
    
    @retry # if first attempt fails, retries 2 more times
    def test():
        x = 1/0 # raises error
        
    @retry(n=5) # if first attempt fails, retries 4 more times
    def test():
        x = 1/0 # raises error
    
    """
    @_contextmanager
    @_wraps(func)
    def wrapper(*args, **kwargs):
        ok = False
        for _ in range(n):
            try:
                yield from func(*args, **kwargs)
                ok=True
                break
            except Exception as e:
                print(f'Function ({func.__name__}) ran into an error while executing: {e}')
                print(f'Retrying in {delay}s ...')
                _sleep(delay)
        if not ok:
            raise RuntimeError(f'Function ({func.__name__}) exceeded number of retries')
    return wrapper

@_deco
def thread_heavy(func=None, max_workers=None, return_val=False):
    """Decorator for distributing thread heavy workload. Can be used on class methods as well as normal functions.
    
    If you have a function defined like the following, where iterables are mixed with non-iterables:
        @thread_heavy
        def work(non_iter1, iter1, non_iter2, iter2):
            do_work()
        
    Use keyword arguments when calling the function:
    work(non_iter1=1, iter1=[1,2], non_iter2=2, iter2=[3,4])
    
    
    Otherwise, the non-iterables need to be before the iterables when you call the function. Example:
        class SomeClass():
            @thread_heavy
            def work(self, non_iter1, non_iter2, iter1, iter2):
                self.do_work()

        with SomeClass() as s:
            s.work(1, 2, [1,2], [3,4])
    
    
    
    :: Parameters ::
    return_val - boolean, if True it will return a list of return values from the function in the order they were completed
    max_workers: integer number of subthreads to run. Default is min(32, os.cpu_count() + 4).
    """
    
    if not max_workers: max_workers = min(32, _cpu_count() + 4)
    
    def find_length(iterable):
        """Finds length of an iterable. Returns the length and a copy of the iterable."""
        try:
            og_iter, t = _tee(iterable)
            length = sum(1 for _ in t)
            i = og_iter
        except TypeError: # not iterable
            length = 1
            i = iterable
        finally:
            return length, i
    
    @_wraps(func)
    def wrapper_execWork(*args, **kwargs):
        # Separates positional arguments into 2 lists: length==1, and length >1
        args = list(args)
        args_len_one = []
        args_iter = []
        for arg in args:
            length, new_arg = find_length(arg)
            if length == 1:
                args_len_one.append(new_arg)
            else:
                args_iter.append(new_arg)
        del args
        
        # Separates kwargs into 2 lists: length==1, and length >1
        kwargs_len_one = dict()
        kwargs_iter = dict()
        for key, arg in kwargs.items():
            length, new_arg = find_length(arg)
            if length == 1:
                kwargs_len_one.update({key: new_arg})
            else:
                kwargs_iter.update({key: new_arg})
        del kwargs
        
        # new function with all args of length 1 inputted
        new_func = _partial(func, *args_len_one, **kwargs_len_one)
        pos_len = len(args_iter)
        
        # multi-threading
        with _PoolExecutor(max_workers=max_workers) as executor:
            try:
                dict_keys, dict_values = zip(*kwargs_iter.items())
                dict_len = len(dict_keys)
                def process_args(args):
                    new_args = args[:-dict_len]
                    new_kwargs = {key: args[i] for i, key in enumerate(dict_keys, pos_len)}
                    return new_args, new_kwargs
                
                fs = [(executor.submit(new_func, *args, **kwargs)) for args, kwargs in map(process_args, zip(*args_iter, *dict_values))]
                    
            except ValueError: # no iterable kwargs
                fs = [executor.submit(new_func, *args) for args in zip(*args_iter)]
        return [fut.result() for fut in fs] if return_val else None
    return wrapper_execWork
