package quickMergeSort;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import quickSort.QuickSort;
import tools.MyArrayUtil;
import tools.MyData;
import tools.MyInteger;

public class QuickMergeSort<E extends Comparable> extends QuickSort<E> {
	ExecutorService executor;
	int threadsNum;

	public QuickMergeSort(){
		threadsNum = Runtime.getRuntime().availableProcessors();
		//threadsNum =4;
		executor = Executors.newCachedThreadPool();

	}

	public void sort(E[] array){

		this.array = array;
		int arrayLength = array.length;
		int sectionOfSort = array.length / threadsNum,pivotOfEnd = 0, pos = 0,pos2 = sectionOfSort - 1;
		int lastLenOfRestSection = -1;  //繰り返し防止を確認するための変数
		int pivotOfRest = -1; //マージするときに余った部分の仕切り
		final List<Callable<Object>> workers = new ArrayList<Callable<Object>>(threadsNum);

		//クイックソートをする
		while(pos2 < arrayLength){
			workers.add(Executors.callable(new QuickSortWorker(pos,pos2)));
			pos = pos2 + 1;
			pos2 =  sectionOfSort + pos2;
		}

		//最後の区分だけ特別に処理する
		workers.add(Executors.callable(new QuickSortWorker(pos,arrayLength-1)));
		pivotOfRest = pos;

		//ここからマージ処理を行う
		try {
			executor.invokeAll(workers);

			int expansionSection = 2;
			pos = 0;pos2 = sectionOfSort * expansionSection - 1 ;
			while(pos2 <= arrayLength - 1){
				workers.clear();

				while(true){
					workers.add(Executors.callable(new MergeSortWorker(pos,pos2)));
					if(pos2 == arrayLength-1)
						break;

					pos = pos2 + 1;
					pivotOfEnd = pos;
					pos2 = pos2 + sectionOfSort*expansionSection;

					if(pos2 > arrayLength-1){
						if(cheakSameSort(arrayLength,pos,lastLenOfRestSection)) break;
						if(pivotOfRest == -1)  //最初のあまり分のとき
							workers.add(Executors.callable(new MergeSortWorker(pos,arrayLength - 1)));
						else
							workers.add(Executors.callable(new MergeSortWorker(pos,pivotOfRest-1,arrayLength - 1)));

						pivotOfRest = pos;
						lastLenOfRestSection = arrayLength - pos;
						break;
					}

				}

				expansionSection = expansionSection * 2;
				executor.invokeAll(workers);
				pos = 0;pos2 = sectionOfSort * expansionSection - 1 ;
			}
			executor.shutdown();
			//最後のmerge
			merge(0,pivotOfEnd-1,arrayLength - 1,new LinkedList<E>());
		} catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
	}

	//前回同じ範囲でソートしたかcheak
	public boolean cheakSameSort(int arrayLength,int pos,int lastLenOfRestSection){
		if((arrayLength - pos) == lastLenOfRestSection)
			return true;

		return false;
	}


	class MergeSortWorker implements Runnable{
		int left,right,mid;
		LinkedList<E> buff;
		public MergeSortWorker(int left,int right){
			this.left = left;
			this.right = right;
			mid = (left + right) / 2;
			buff = new LinkedList<E>();
		}

		public MergeSortWorker(int left,int mid,int right){
			this.left = left;
			this.right = right;
			this.mid = mid;
			buff = new LinkedList<E>();
		}
		public void run(){
			merge(left,mid,right,buff);
		}
	}

	public  void merge(int left,int mid,int right,LinkedList<E> buff){
		int i = left,j = mid + 1;

		while(i <= mid && j <= right) {
			if(array[i].compareTo(array[j]) < 0){
				buff.add(array[i]); i++;
			}else{
				buff.add(array[j]); j++;
			}
		}

		while(i <= mid) { buff.add(array[i]); i++;}
		while(j <= right) { buff.add(array[j]); j++;}
		for(i = left;i <= right; i++){ array[i] = buff.remove(0);}
	}

	class QuickSortWorker implements Runnable {
		int left,right;
		public QuickSortWorker(int left,int right){
			this.left = left;
			this.right = right;
		}

		public void run() {
			quickSort(left,right);
		}
	}

	//	public  void quickSort(int left,int right){
	//		super.quickSort(left, right);
	//	}


	public void p(String st,int i,String st2 ,int j){
		System.out.print("            "+st+" : "+i);
		System.out.println("      "+st2+" : "+j);
	}
	public void p(String st,int i){
		System.out.println("            "+st+" : "+i);
	}

	public void p(String st){
		System.out.println("            "+st+":");
	}
}
